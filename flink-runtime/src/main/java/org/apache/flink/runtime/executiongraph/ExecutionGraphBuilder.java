/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link
 * JobGraph}.
 */
public class ExecutionGraphBuilder {

    /**
     * Builds the ExecutionGraph from the JobGraph. If a prior execution graph exists, the JobGraph
     * will be attached. If no prior execution graph exists, then the JobGraph will become attach to
     * a new empty execution graph.
     */
    @VisibleForTesting
    public static ExecutionGraph buildGraph(@Nullable ExecutionGraph prior, JobGraph jobGraph,
            Configuration jobManagerConfig, ScheduledExecutorService futureExecutor, Executor ioExecutor,
            SlotProvider slotProvider, ClassLoader classLoader, CheckpointRecoveryFactory recoveryFactory,
            Time rpcTimeout, RestartStrategy restartStrategy, MetricGroup metrics, BlobWriter blobWriter,
            Time allocationTimeout, Logger log, ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            long initializationTimestamp) throws JobExecutionException, JobException {

        final FailoverStrategy.Factory failoverStrategy = FailoverStrategyLoader
                .loadFailoverStrategy(jobManagerConfig, log);

        return buildGraph(prior, jobGraph, jobManagerConfig, futureExecutor, ioExecutor, slotProvider, classLoader,
                recoveryFactory, rpcTimeout, restartStrategy, metrics, blobWriter, allocationTimeout, log, shuffleMaster,
                partitionTracker, failoverStrategy, NoOpExecutionDeploymentListener.get(), (execution, newState) -> {
                }, initializationTimestamp);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 此方法，帮助我们完成从 JobGraph 到 ExecutionGraph 的转换
     */
    public static ExecutionGraph buildGraph(@Nullable ExecutionGraph prior, JobGraph jobGraph,
            Configuration jobManagerConfig, ScheduledExecutorService futureExecutor, Executor ioExecutor,
            SlotProvider slotProvider, ClassLoader classLoader, CheckpointRecoveryFactory recoveryFactory,
            Time rpcTimeout, RestartStrategy restartStrategy, MetricGroup metrics, BlobWriter blobWriter,
            Time allocationTimeout, Logger log, ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker, FailoverStrategy.Factory failoverStrategyFactory,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp) throws JobExecutionException, JobException {

        checkNotNull(jobGraph, "job graph cannot be null");

        final String jobName = jobGraph.getName();
        final JobID jobId = jobGraph.getJobID();

        final JobInformation jobInformation = new JobInformation(jobId, jobName, jobGraph.getSerializedExecutionConfig(),
                jobGraph.getJobConfiguration(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());

        final int maxPriorAttemptsHistoryLength = jobManagerConfig
                .getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

        final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory = PartitionReleaseStrategyFactoryLoader
                .loadPartitionReleaseStrategyFactory(jobManagerConfig);

        // create a new execution graph, if none exists so far
        final ExecutionGraph executionGraph;
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 此时，只是初始化了这个 ExecutionGraph 空壳
             */
            executionGraph = (prior != null) ? prior : new ExecutionGraph(jobInformation, futureExecutor, ioExecutor,
                    rpcTimeout, restartStrategy, maxPriorAttemptsHistoryLength, failoverStrategyFactory, slotProvider,
                    classLoader, blobWriter, allocationTimeout, partitionReleaseStrategyFactory, shuffleMaster,
                    partitionTracker, jobGraph.getScheduleMode(), executionDeploymentListener,
                    executionStateUpdateListener, initializationTimestamp);
        } catch(IOException e) {
            throw new JobException("Could not create the ExecutionGraph.", e);
        }

        // set the basic properties

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 生成 JobGraph 的 JSON 表达形式
         */
        try {
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch(Throwable t) {
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits

        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 做校验
         */
        for(JobVertex vertex : jobGraph.getVertices()) {
            String executableClass = vertex.getInvokableClassName();
            if(executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(jobId,
                        "The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
            }
            try {
                vertex.initializeOnMaster(classLoader);
            } catch(Throwable t) {
                throw new JobExecutionException(jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
            }
        }

        log.info("Successfully ran initialization on master in {} ms.",
                (System.nanoTime() - initMasterStart) / 1_000_000);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 最重要的事情，生成 ExecutionJobVertex 以及并行化，根据并行度，生成多个 ExecutionVertex
         *  注释翻译： 对作业顶点进行拓扑排序，然后将图形附加到现有的顶点上
         */
        // topologically sort the job vertices and attach the graph to the existing one
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        if(log.isDebugEnabled()) {
            log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(), jobName, jobId);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 各种重点细节，都在这个里面
         */
        executionGraph.attachJobGraph(sortedTopology);

        if(log.isDebugEnabled()) {
            log.debug("Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 解析 checkpoint 参数，构建 checkpoint 相关各种组件
         */
        // configure the state checkpointing
        JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();

        // TODO_MA 注释： 如果设置了 checkpoint， 则进行解析
        // TODO_MA 注释： 将  JobGraph 中的 JobVertex 变成 ExecutionJobVertex
        if(snapshotSettings != null) {
            List<ExecutionJobVertex> triggerVertices = idToVertex(snapshotSettings.getVerticesToTrigger(),
                    executionGraph);
            List<ExecutionJobVertex> ackVertices = idToVertex(snapshotSettings.getVerticesToAcknowledge(),
                    executionGraph);
            List<ExecutionJobVertex> confirmVertices = idToVertex(snapshotSettings.getVerticesToConfirm(),
                    executionGraph);

            // TODO_MA 注释： 创建 CompletedCheckpointStore 和 CheckpointIDCounter
            CompletedCheckpointStore completedCheckpoints;
            CheckpointIDCounter checkpointIdCounter;
            try {
                int maxNumberOfCheckpointsToRetain = jobManagerConfig
                        .getInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

                if(maxNumberOfCheckpointsToRetain <= 0) {
                    // warning and use 1 as the default value if the setting in
                    // state.checkpoints.max-retained-checkpoints is not greater than 0.
                    log.warn("The setting for '{} : {}' is invalid. Using default value of {}",
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(), maxNumberOfCheckpointsToRetain,
                            CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

                    maxNumberOfCheckpointsToRetain = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
                }

                // TODO_MA 注释： recoveryFactory = ZooKeeperCheckpointRecoveryFactory
                // TODO_MA 注释： 创建 DefaultCompletedCheckpointStore 和 ZooKeeperCheckpointIDCounter
                completedCheckpoints = recoveryFactory
                        .createCheckpointStore(jobId, maxNumberOfCheckpointsToRetain, classLoader);
                checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);

            } catch(Exception e) {
                throw new JobExecutionException(jobId, "Failed to initialize high-availability checkpoint handler", e);
            }

            // Maximum number of remembered checkpoints
            int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： CheckpointStatsTracker
             */
            CheckpointStatsTracker checkpointStatsTracker = new CheckpointStatsTracker(historySize, ackVertices,
                    snapshotSettings.getCheckpointCoordinatorConfiguration(), metrics);

            // load the state backend from the application settings
            final StateBackend applicationConfiguredBackend;
            final SerializedValue<StateBackend> serializedAppConfigured = snapshotSettings.getDefaultStateBackend();

            if(serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try {
                    applicationConfiguredBackend = serializedAppConfigured.deserializeValue(classLoader);
                } catch(IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            // TODO_MA 注释： 获取 StateBackend
            final StateBackend rootBackend;
            try {
                rootBackend = StateBackendLoader
                        .fromApplicationOrConfigOrDefault(applicationConfiguredBackend, jobManagerConfig, classLoader,
                                log);
            } catch(IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(jobId, "Could not instantiate configured state backend", e);
            }

            // instantiate the user-defined checkpoint hooks

            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks = snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if(serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch(IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for(MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 获取 CheckpointCoordinatorConfiguration
             */
            final CheckpointCoordinatorConfiguration chkConfig = snapshotSettings.getCheckpointCoordinatorConfiguration();

            // TODO_MA 注释： 启用 checkpoint 相关参数
            // TODO_MA 注释： 这句代码的内部，会创建 CheckpointCoordinator
            executionGraph.enableCheckpointing(chkConfig, triggerVertices, ackVertices, confirmVertices, hooks,
                    checkpointIdCounter, completedCheckpoints, rootBackend, checkpointStatsTracker);
        }

        // create all the metrics for the Execution Graph

        metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
        metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
        metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));

        executionGraph.getFailoverStrategy().registerMetrics(metrics);

        return executionGraph;
    }

    private static List<ExecutionJobVertex> idToVertex(List<JobVertexID> jobVertices,
            ExecutionGraph executionGraph) throws IllegalArgumentException {

        // TODO_MA 注释： JobVertex 并行化成多个 ExecutionVertex
        // TODO_MA 注释： 最后，每个 ExecutionVertex 事实上，会运行一个 Task
        List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

        for(JobVertexID id : jobVertices) {

            // TODO_MA 注释： 其实是一对一的转换关系
            ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
            if(vertex != null) {

                // TODO_MA 注释： 加入集合
                result.add(vertex);
            } else {
                throw new IllegalArgumentException(
                        "The snapshot checkpointing settings refer to non-existent vertex " + id);
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------

    /**
     * This class is not supposed to be instantiated.
     */
    private ExecutionGraphBuilder() {
    }
}
