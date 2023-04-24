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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation.ResolutionMode;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single {@link
 * JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 *
 * <ul>
 *   <li>{@link #updateTaskExecutionState} updates the task execution state for given task
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId> implements JobMasterGateway, JobMasterService {

    /**
     * Default names for Flink's distributed components.
     */
    public static final String JOB_MANAGER_NAME = "jobmanager";

    // ------------------------------------------------------------------------

    private final JobMasterConfiguration jobMasterConfiguration;

    private final ResourceID resourceId;

    private final JobGraph jobGraph;

    private final Time rpcTimeout;

    private final HighAvailabilityServices highAvailabilityServices;

    private final BlobWriter blobWriter;

    private final HeartbeatServices heartbeatServices;

    private final JobManagerJobMetricGroupFactory jobMetricGroupFactory;

    private final ScheduledExecutorService scheduledExecutorService;

    private final OnCompletionActions jobCompletionActions;

    private final FatalErrorHandler fatalErrorHandler;

    private final ClassLoader userCodeLoader;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 持有和管理 分配到该 Job 的所有资源
     *  默认实现： SlotPoolImpl
     */
    private final SlotPool slotPool;

    private final SchedulerNGFactory schedulerNGFactory;

    private final long initializationTimestamp;

    private final boolean retrieveTaskManagerHostName;

    // --------- BackPressure --------
    private final BackPressureStatsTracker backPressureStatsTracker;

    // --------- ResourceManager --------
    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // --------- TaskManagers --------
    private final Map<ResourceID, Tuple2<TaskManagerLocation, TaskExecutorGateway>> registeredTaskManagers;

    private final ShuffleMaster<?> shuffleMaster;

    // -------- Mutable fields ---------

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> taskManagerHeartbeatManager;

    private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： Scheduler 组件负责调度执行该 Job 的 DAG 中所有 Task，发出资源请求，即整个资源调度的起点
     *  实现类： DefaultScheduler
     *  作用：
     *  1、负责调度执行该 Job 的 DAG 中所有 Task
     *  2、发起 SlotRequest 资源请求
     *  3、决定 Task / Slot 之间的分配
     */
    private SchedulerNG schedulerNG;

    @Nullable
    private JobManagerJobStatusListener jobStatusListener;

    @Nullable
    private JobManagerJobMetricGroup jobManagerJobMetricGroup;

    @Nullable
    private ResourceManagerAddress resourceManagerAddress;

    @Nullable
    private ResourceManagerConnection resourceManagerConnection;

    @Nullable
    private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    private Map<String, Object> accumulators;

    private final JobMasterPartitionTracker partitionTracker;

    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final ExecutionDeploymentReconciler executionDeploymentReconciler;

    // ------------------------------------------------------------------------

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 转到 onStart() 生命周期方法
     *  在一个 JobMaster 初始化之后运行的过程中，需要做哪些事情呢？
     *  1、一定要像 ResourceManager 注册，并且维持心跳
     *  2、解析 JobGraph 得到 ExecutorGraph ，其实 ExecutorGraph 就是 JobGraph 的并行化版本
     *  3、JobMaster 负责向 ResourceManager 去申请 slot （一个 slot 启动一个 StreamTask）
     *  4、派发任务运行，并且监控他们的状态
     *  5、维持 JobMaster 和 StreamTask 之间的心跳
     *  6、JobMaster 还需要进行 ZooKeeper 相关操作
     */
    public JobMaster(RpcService rpcService, JobMasterConfiguration jobMasterConfiguration, ResourceID resourceId,
            JobGraph jobGraph, HighAvailabilityServices highAvailabilityService, SlotPoolFactory slotPoolFactory,
            JobManagerSharedServices jobManagerSharedServices, HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobMetricGroupFactory, OnCompletionActions jobCompletionActions,
            FatalErrorHandler fatalErrorHandler, ClassLoader userCodeLoader, SchedulerNGFactory schedulerNGFactory,
            ShuffleMaster<?> shuffleMaster, PartitionTrackerFactory partitionTrackerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ExecutionDeploymentReconciler.Factory executionDeploymentReconcilerFactory,
            long initializationTimestamp) throws Exception {

        // TODO_MA 注释： 开启 RPC 服务
        super(rpcService, AkkaRpcServiceUtils.createRandomName(JOB_MANAGER_NAME), null);

        final ExecutionDeploymentReconciliationHandler executionStateReconciliationHandler = new ExecutionDeploymentReconciliationHandler() {

            @Override
            public void onMissingDeploymentsOf(Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                log.debug("Failing deployments {} due to no longer being deployed.", executionAttemptIds);
                for(ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                    schedulerNG.updateTaskExecutionState(
                            new TaskExecutionState(jobGraph.getJobID(), executionAttemptId, ExecutionState.FAILED,
                                    new FlinkException(String.format(
                                            "Execution %s is unexpectedly no longer running on task executor %s.",
                                            executionAttemptId, host))));
                }
            }

            @Override
            public void onUnknownDeploymentsOf(Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                log.debug("Canceling left-over deployments {} on task executor {}.", executionAttemptIds, host);
                for(ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                    Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerInfo = registeredTaskManagers.get(host);
                    if(taskManagerInfo != null) {
                        taskManagerInfo.f1.cancelTask(executionAttemptId, rpcTimeout);
                    }
                }
            }
        };

        this.executionDeploymentTracker = executionDeploymentTracker;
        this.executionDeploymentReconciler = executionDeploymentReconcilerFactory
                .create(executionStateReconciliationHandler);

        this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
        this.resourceId = checkNotNull(resourceId);

        // TODO_MA 注释： JobGraph 被保存在 JobMaster 中，之后会被构建成 ExeuctionGraph
        this.jobGraph = checkNotNull(jobGraph);

        this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        this.blobWriter = jobManagerSharedServices.getBlobWriter();
        this.scheduledExecutorService = jobManagerSharedServices.getScheduledExecutorService();
        this.jobCompletionActions = checkNotNull(jobCompletionActions);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.schedulerNGFactory = checkNotNull(schedulerNGFactory);
        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.jobMetricGroupFactory = checkNotNull(jobMetricGroupFactory);
        this.initializationTimestamp = initializationTimestamp;
        this.retrieveTaskManagerHostName = jobMasterConfiguration.getConfiguration()
                .getBoolean(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);

        final String jobName = jobGraph.getName();
        final JobID jid = jobGraph.getJobID();

        log.info("Initializing job {} ({}).", jobName, jid);

        // TODO_MA 注释： resourceManager Leader 地址接收器
        resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

        // TODO_MA 注释： 创建 SlotPool： 负责该job的slot申请和释放等slot管理工作
        // TODO_MA 注释： 创建一个 SlotPool 对象
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 其实跟资源slot有关的东西有三个组件：
         *  1、ResourceManager   slot管理者      SlotManagerImpl
         *  2、TaskExecutor      slot提供者      TaskSlotTable
         *  3、JobMaster         slot使用者      SlotPool
         */
        this.slotPool = checkNotNull(slotPoolFactory).createSlotPool(jid);

        this.registeredTaskManagers = new HashMap<>(4);
        this.partitionTracker = checkNotNull(partitionTrackerFactory).create(resourceID -> {
            Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerInfo = registeredTaskManagers.get(resourceID);
            if(taskManagerInfo == null) {
                return Optional.empty();
            }

            return Optional.of(taskManagerInfo.f1);
        });

        this.backPressureStatsTracker = checkNotNull(jobManagerSharedServices.getBackPressureStatsTracker());

        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： DefaultScheduler
         *  内部会将 JobGraph 变成 ExecutionGraph
         */
        this.schedulerNG = createScheduler(executionDeploymentTracker, jobManagerJobMetricGroup);

        this.jobStatusListener = null;

        this.resourceManagerConnection = null;
        this.establishedResourceManagerConnection = null;

        this.accumulators = new HashMap<>();
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
    }

    private SchedulerNG createScheduler(ExecutionDeploymentTracker executionDeploymentTracker,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup) throws Exception {

        // TODO_MA 注释： DefaultScheduler
        // TODO_MA 注释： CheckpointRecoveryFactory = ZooKeeperCheckpointRecoveryFactory
        return schedulerNGFactory.createInstance(log, jobGraph, backPressureStatsTracker, scheduledExecutorService,
                jobMasterConfiguration.getConfiguration(), slotPool, scheduledExecutorService, userCodeLoader,
                highAvailabilityServices.getCheckpointRecoveryFactory(), rpcTimeout, blobWriter, jobManagerJobMetricGroup,
                jobMasterConfiguration.getSlotRequestTimeout(), shuffleMaster, partitionTracker,
                executionDeploymentTracker, initializationTimestamp);
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------

    /**
     * Start the rpc service and begin to run the job.
     *
     * @param newJobMasterId The necessary fencing token to run the job
     * @return Future acknowledge if the job could be started. Otherwise the future contains an
     * exception
     */
    public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {

        // TODO_MA 注释： 启动 RpcServer
        // TODO_MA 注释： 由于 JobMaster 是一个 RpcEndpoint，
        // TODO_MA 注释： 所以 自此，跳转到 onStart() 方法, 只是 jobMaster 并没有重写 onStart()
        // make sure we receive RPC and async calls
        start();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、JobMaster 向 ResourceManager 注册
         *  2、开始申请 slot 并且调度部署 StreamTask
         */
        return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
    }

    /**
     * Suspending job, all the running tasks will be cancelled, and communication with other
     * components will be disposed.
     *
     * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart
     * this job by calling the {@link #start(JobMasterId)} method once we take the leadership back
     * again.
     *
     * <p>This method is executed asynchronously
     *
     * @param cause The reason of why this job been suspended.
     * @return Future acknowledge indicating that the job has been suspended. Otherwise the future
     * contains an exception
     */
    public CompletableFuture<Acknowledge> suspend(final Exception cause) {
        CompletableFuture<Acknowledge> suspendFuture = callAsyncWithoutFencing(() -> suspendExecution(cause),
                RpcUtils.INF_TIMEOUT);

        return suspendFuture.whenComplete((acknowledge, throwable) -> stop());
    }

    /**
     * Suspend the job and shutdown all other services including rpc.
     */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping the JobMaster for job {}({}).", jobGraph.getName(), jobGraph.getJobID());

        // make sure there is a graceful exit
        suspendExecution(new FlinkException(
                "Stopping JobMaster for job " + jobGraph.getName() + '(' + jobGraph.getJobID() + ")."));

        // shut down will internally release all registered slots
        slotPool.close();

        return CompletableFuture.completedFuture(null);
    }

    // ----------------------------------------------------------------------------------------------
    // RPC methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        schedulerNG.cancel();

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task
     * @return Acknowledge the task execution state update
     */
    @Override
    public CompletableFuture<Acknowledge> updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
        checkNotNull(taskExecutionState, "taskExecutionState");

        if(schedulerNG.updateTaskExecutionState(taskExecutionState)) {
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            return FutureUtils.completedExceptionally(new ExecutionGraphException(
                    "The execution attempt " + taskExecutionState.getID() + " was not found."));
        }
    }

    @Override
    public CompletableFuture<SerializedInputSplit> requestNextInputSplit(final JobVertexID vertexID,
            final ExecutionAttemptID executionAttempt) {

        try {
            return CompletableFuture.completedFuture(schedulerNG.requestNextInputSplit(vertexID, executionAttempt));
        } catch(IOException e) {
            log.warn("Error while requesting next input split", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<ExecutionState> requestPartitionState(final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId) {

        try {
            return CompletableFuture
                    .completedFuture(schedulerNG.requestPartitionState(intermediateResultId, resultPartitionId));
        } catch(PartitionProducerDisposedException e) {
            log.info("Error while requesting partition state", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(final ResultPartitionID partitionID,
            final Time timeout) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        schedulerNG.scheduleOrUpdateConsumers(partitionID);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectTaskManager(final ResourceID resourceID, final Exception cause) {
        log.debug("Disconnect TaskExecutor {} because: {}", resourceID.getStringWithMetadata(), cause.getMessage());

        taskManagerHeartbeatManager.unmonitorTarget(resourceID);
        slotPool.releaseTaskManager(resourceID, cause);
        partitionTracker.stopTrackingPartitionsFor(resourceID);

        Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerConnection = registeredTaskManagers
                .remove(resourceID);

        if(taskManagerConnection != null) {
            taskManagerConnection.f1.disconnectJobManager(jobGraph.getJobID(), cause);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // TODO: This method needs a leader session ID
    @Override
    public void acknowledgeCheckpoint(final JobID jobID, final ExecutionAttemptID executionAttemptID,
            final long checkpointId, final CheckpointMetrics checkpointMetrics, final TaskStateSnapshot checkpointState) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 调用 Scheduler 来处理 ack 消息
         *  意义： 哪个 jobID 中的 哪个 executionAttemptID 对应的 Task 的第 checkpointId 次 checkpoint 成功了
         */
        schedulerNG.acknowledgeCheckpoint(jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);
    }

    // TODO: This method needs a leader session ID
    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        schedulerNG.declineCheckpoint(decline);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(final ExecutionAttemptID task,
            final OperatorID operatorID, final SerializedValue<OperatorEvent> serializedEvent) {

        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            final OperatorEvent evt = serializedEvent.deserializeValue(userCodeLoader);
            schedulerNG.deliverOperatorEventToCoordinator(task, operatorID, evt);

            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch(Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<KvStateLocation> requestKvStateLocation(final JobID jobId, final String registrationName) {
        try {
            return CompletableFuture.completedFuture(schedulerNG.requestKvStateLocation(jobId, registrationName));
        } catch(UnknownKvStateLocation | FlinkJobNotFoundException e) {
            log.info("Error while request key-value state location", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateRegistered(final JobID jobId, final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange, final String registrationName, final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress) {

        try {
            schedulerNG.notifyKvStateRegistered(jobId, jobVertexId, keyGroupRange, registrationName, kvStateId,
                    kvStateServerAddress);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch(FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateUnregistered(JobID jobId, JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange, String registrationName) {
        try {
            schedulerNG.notifyKvStateUnregistered(jobId, jobVertexId, keyGroupRange, registrationName);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch(FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state de-registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    // TODO_MA 注释： 完成接受处理
    @Override
    public CompletableFuture<Collection<SlotOffer>> offerSlots(final ResourceID taskManagerId,
            final Collection<SlotOffer> slots, final Time timeout) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取 TaskExecutor 的位置 和 GateWay
         */
        Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManager = registeredTaskManagers.get(taskManagerId);

        if(taskManager == null) {
            return FutureUtils.completedExceptionally(new Exception("Unknown TaskManager " + taskManagerId));
        }

        final TaskManagerLocation taskManagerLocation = taskManager.f0;
        final TaskExecutorGateway taskExecutorGateway = taskManager.f1;

        final RpcTaskManagerGateway rpcTaskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway,
                getFencingToken());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将 TaskExecutor 汇报过来的 AllocatedSlot 加入 SlotPool 进行管理
         */
        return CompletableFuture.completedFuture(slotPool.offerSlots(taskManagerLocation, rpcTaskManagerGateway, slots));
    }

    @Override
    public void failSlot(final ResourceID taskManagerId, final AllocationID allocationId, final Exception cause) {

        if(registeredTaskManagers.containsKey(taskManagerId)) {
            internalFailAllocation(allocationId, cause);
        } else {
            log.warn("Cannot fail slot " + allocationId + " because the TaskManager " + taskManagerId + " is unknown.");
        }
    }

    private void internalFailAllocation(AllocationID allocationId, Exception cause) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 申请失败
         */
        final Optional<ResourceID> resourceIdOptional = slotPool.failAllocation(allocationId, cause);

        resourceIdOptional.ifPresent(taskManagerId -> {
            if(!partitionTracker.isTrackingPartitionsFor(taskManagerId)) {
                releaseEmptyTaskManager(taskManagerId);
            }
        });
    }

    private void releaseEmptyTaskManager(ResourceID resourceId) {
        disconnectTaskManager(resourceId, new FlinkException(
                String.format("No more slots registered at JobMaster %s.", resourceId.getStringWithMetadata())));
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerTaskManager(final String taskManagerRpcAddress,
            final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation, final Time timeout) {

        final TaskManagerLocation taskManagerLocation;
        try {
            if(retrieveTaskManagerHostName) {
                taskManagerLocation = TaskManagerLocation
                        .fromUnresolvedLocation(unresolvedTaskManagerLocation, ResolutionMode.RETRIEVE_HOST_NAME);
            } else {
                taskManagerLocation = TaskManagerLocation
                        .fromUnresolvedLocation(unresolvedTaskManagerLocation, ResolutionMode.USE_IP_ONLY);
            }
        } catch(Throwable throwable) {
            final String errMsg = String
                    .format("Could not accept TaskManager registration. TaskManager address %s cannot be resolved. %s",
                            unresolvedTaskManagerLocation.getExternalAddress(), throwable.getMessage());
            log.error(errMsg);
            return CompletableFuture.completedFuture(new RegistrationResponse.Decline(errMsg));
        }

        final ResourceID taskManagerId = taskManagerLocation.getResourceID();

        if(registeredTaskManagers.containsKey(taskManagerId)) {
            final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
            return CompletableFuture.completedFuture(response);
        } else {

            // TODO_MA 注释： 链接 TaskExecutor
            return getRpcService().connect(taskManagerRpcAddress, TaskExecutorGateway.class)
                    .handleAsync((TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
                        if(throwable != null) {
                            return new RegistrationResponse.Decline(throwable.getMessage());
                        }

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 注册 TaskExecutor
                         */
                        slotPool.registerTaskManager(taskManagerId);
                        registeredTaskManagers.put(taskManagerId, Tuple2.of(taskManagerLocation, taskExecutorGateway));

                        // TODO_MA 注释： 管理心跳
                        // monitor the task manager as heartbeat target
                        taskManagerHeartbeatManager
                                .monitorTarget(taskManagerId, new HeartbeatTarget<AllocatedSlotReport>() {
                                    @Override
                                    public void receiveHeartbeat(ResourceID resourceID, AllocatedSlotReport payload) {
                                        // the task manager will not request heartbeat, so
                                        // this method will never be called currently
                                    }

                                    @Override
                                    public void requestHeartbeat(ResourceID resourceID,
                                            AllocatedSlotReport allocatedSlotReport) {
                                        taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
                                    }
                                });

                        return new JMTMRegistrationSuccess(resourceId);
                    }, getMainThreadExecutor());
        }
    }

    @Override
    public void disconnectResourceManager(final ResourceManagerId resourceManagerId, final Exception cause) {

        if(isConnectingToResourceManager(resourceManagerId)) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            reconnectToResourceManager(cause);
        }
    }

    private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
        return resourceManagerAddress != null && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
    }

    @Override
    public void heartbeatFromTaskManager(final ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： JobMaster 接收到来自 TaskExecutor 的心跳响应
         */
        taskManagerHeartbeatManager.receiveHeartbeat(resourceID, payload);
    }

    @Override
    public void heartbeatFromResourceManager(final ResourceID resourceID) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 当 jobmaster 接收到 rm 的心跳请求的时候，执行处理
         */
        resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobDetails());
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobStatus());
    }

    @Override
    public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJob());
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(@Nullable final String targetDirectory, final boolean cancelJob,
            final Time timeout) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return schedulerNG.triggerSavepoint(targetDirectory, cancelJob);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(@Nullable final String targetDirectory,
            final boolean advanceToEndOfEventTime, final Time timeout) {

        return schedulerNG.stopWithSavepoint(targetDirectory, advanceToEndOfEventTime);
    }

    @Override
    public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
            final JobVertexID jobVertexId) {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            final Optional<OperatorBackPressureStats> operatorBackPressureStats = schedulerNG
                    .requestOperatorBackPressureStats(jobVertexId);
            return CompletableFuture
                    .completedFuture(OperatorBackPressureStatsResponse.of(operatorBackPressureStats.orElse(null)));
        } catch(FlinkException e) {
            log.info("Error while requesting operator back pressure stats", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        internalFailAllocation(allocationID, cause);
    }

    @Override
    public CompletableFuture<Object> updateGlobalAggregate(String aggregateName, Object aggregand,
            byte[] serializedAggregateFunction) {

        AggregateFunction aggregateFunction = null;
        try {
            aggregateFunction = InstantiationUtil.deserializeObject(serializedAggregateFunction, userCodeLoader);
        } catch(Exception e) {
            log.error("Error while attempting to deserialize user AggregateFunction.");
            return FutureUtils.completedExceptionally(e);
        }

        Object accumulator = accumulators.get(aggregateName);
        if(null == accumulator) {
            accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add(aggregand, accumulator);
        accumulators.put(aggregateName, accumulator);
        return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest, Time timeout) {
        try {
            CoordinationRequest request = serializedRequest.deserializeValue(userCodeLoader);
            return schedulerNG.deliverCoordinationRequestToCoordinator(operatorId, request);
        } catch(Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Internal methods
    // ----------------------------------------------------------------------------------------------

    // -- job starting and stopping
    // -----------------------------------------------------------------

    private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {

        validateRunsInMainThread();

        checkNotNull(newJobMasterId, "The new JobMasterId must not be null.");

        if(Objects.equals(getFencingToken(), newJobMasterId)) {
            log.info("Already started the job execution with JobMasterId {}.", newJobMasterId);

            return Acknowledge.get();
        }

        setNewFencingToken(newJobMasterId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动一些服务
         */
        startJobMasterServices();

        log.info("Starting execution of job {} ({}) under job master id {}.", jobGraph.getName(), jobGraph.getJobID(),
                newJobMasterId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 解析 ExecutionGraph，申请Slot，部署 Task 到 TaskExecutor
         */
        resetAndStartScheduler();

        return Acknowledge.get();
    }

    private void startJobMasterServices() throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一步： 启动心跳服务
         *  JobMaster 需要跟 ResourceManager 也需要跟 TaskExecutor 建立心跳
         *  -
         *  关于这个心跳机制：
         *  1、心跳是一个双向的，一方主动发起心跳请求，另外一方响应，发回反馈
         *  2、参与心跳的有三大组件： ResourceManager TaskExecutor JobMaster
         *  -
         *  ResourceManager(主动) TaskExecutor（被动）
         *  ResourceManager(主动)  JobMaster（被动）
         *  JobMaster(主动)       TaskExecutor（被动）
         */
        startHeartbeatServices();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第二步： 启动启动 Slot 管理服务，内部启动了 3 个定时任务
         */
        // start the slot pool make sure the slot pool now accepts messages for this leader
        slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第三步： 链接 ResourceManager 进行 JobMaster 的注册
         */
        // TODO: Remove once the ZooKeeperLeaderRetrieval returns the stored address upon start
        // try to reconnect to previously known leader
        reconnectToResourceManager(new FlinkException("Starting JobMaster component."));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第四步： 监听 ResourceManager 的地址更改
         */
        // job is ready to go, try to establish connection with resource manager
        //   - activate leader retrieval for the resource manager
        //   - on notification of the leader, the connection will be established and
        //     the slot pool will start requesting slots
        resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
    }

    private void setNewFencingToken(JobMasterId newJobMasterId) {
        if(getFencingToken() != null) {
            log.info("Restarting old job with JobMasterId {}. The new JobMasterId is {}.", getFencingToken(),
                    newJobMasterId);

            // first we have to suspend the current execution
            suspendExecution(new FlinkException(
                    "Old job with JobMasterId " + getFencingToken() + " is restarted with a new JobMasterId " + newJobMasterId + '.'));
        }

        // set new leader id
        setFencingToken(newJobMasterId);
    }

    /**
     * Suspending job, all the running tasks will be cancelled, and communication with other
     * components will be disposed.
     *
     * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart
     * this job by calling the {@link #start(JobMasterId)} method once we take the leadership back
     * again.
     *
     * @param cause The reason of why this job been suspended.
     */
    private Acknowledge suspendExecution(final Exception cause) {
        validateRunsInMainThread();

        if(getFencingToken() == null) {
            log.debug("Job has already been suspended or shutdown.");
            return Acknowledge.get();
        }

        // not leader anymore --> set the JobMasterId to null
        setFencingToken(null);

        try {
            resourceManagerLeaderRetriever.stop();
            resourceManagerAddress = null;
        } catch(Throwable t) {
            log.warn("Failed to stop resource manager leader retriever when suspending.", t);
        }

        suspendAndClearSchedulerFields(cause);

        // disconnect from all registered TaskExecutors
        final Set<ResourceID> taskManagerResourceIds = new HashSet<>(registeredTaskManagers.keySet());

        for(ResourceID taskManagerResourceId : taskManagerResourceIds) {
            disconnectTaskManager(taskManagerResourceId, cause);
        }

        // the slot pool stops receiving messages and clears its pooled slots
        slotPool.suspend();

        // disconnect from resource manager:
        closeResourceManagerConnection(cause);

        stopHeartbeatServices();

        return Acknowledge.get();
    }

    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        resourceManagerHeartbeatManager.stop();
    }

    private void startHeartbeatServices() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 维持和 TaskExecutor 之间的心跳
         *  JobMaster(主动)       TaskExecutor（被动）
         */
        taskManagerHeartbeatManager = heartbeatServices
                .createHeartbeatManagerSender(resourceId, new TaskManagerHeartbeatListener(), getMainThreadExecutor(),
                        log);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 维持 和 ResourceManager 之间的心跳
         *  ResourceManager(主动)  JobMaster（被动）
         */
        resourceManagerHeartbeatManager = heartbeatServices
                .createHeartbeatManager(resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private void assignScheduler(SchedulerNG newScheduler, JobManagerJobMetricGroup newJobManagerJobMetricGroup) {
        validateRunsInMainThread();
        checkState(schedulerNG.requestJobStatus().isTerminalState());
        checkState(jobManagerJobMetricGroup == null);

        schedulerNG = newScheduler;
        jobManagerJobMetricGroup = newJobManagerJobMetricGroup;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： JobMaster 内部的开始调度入口方法
     */
    private void resetAndStartScheduler() throws Exception {
        validateRunsInMainThread();

        final CompletableFuture<Void> schedulerAssignedFuture;

        // TODO_MA 注释： Job 的默认初始状态是：JobStatus.CREATED
        if(schedulerNG.requestJobStatus() == JobStatus.CREATED) {
            schedulerAssignedFuture = CompletableFuture.completedFuture(null);
            schedulerNG.setMainThreadExecutor(getMainThreadExecutor());
        } else {
            suspendAndClearSchedulerFields(
                    new FlinkException("ExecutionGraph is being reset in order to be rescheduled."));
            final JobManagerJobMetricGroup newJobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
            final SchedulerNG newScheduler = createScheduler(executionDeploymentTracker, newJobManagerJobMetricGroup);

            schedulerAssignedFuture = schedulerNG.getTerminationFuture().handle((ignored, throwable) -> {
                newScheduler.setMainThreadExecutor(getMainThreadExecutor());
                assignScheduler(newScheduler, newJobManagerJobMetricGroup);
                return null;
            });
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 开始调度
         */
        FutureUtils.assertNoException(schedulerAssignedFuture.thenRun(this::startScheduling));
    }

    private void startScheduling() {
        checkState(jobStatusListener == null);
        // register self as job status change listener

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 注册一个 JobStatus 的 监听器，监听 job 的状态
         */
        jobStatusListener = new JobManagerJobStatusListener();
        schedulerNG.registerJobStatusListener(jobStatusListener);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 开始调度
         *  schedulerNG = DefaultScheduler
         *  DefaultScheduler 是 SchedulerBase 的子类
         */
        schedulerNG.startScheduling();
    }

    private void suspendAndClearSchedulerFields(Exception cause) {
        suspendScheduler(cause);
        clearSchedulerFields();
    }

    private void suspendScheduler(Exception cause) {
        schedulerNG.suspend(cause);

        if(jobManagerJobMetricGroup != null) {
            jobManagerJobMetricGroup.close();
        }

        if(jobStatusListener != null) {
            jobStatusListener.stop();
        }
    }

    private void clearSchedulerFields() {
        jobManagerJobMetricGroup = null;
        jobStatusListener = null;
    }

    // ----------------------------------------------------------------------------------------------

    private void handleJobMasterError(final Throwable cause) {
        if(ExceptionUtils.isJvmFatalError(cause)) {
            log.error("Fatal error occurred on JobManager.", cause);
            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        } else {
            jobCompletionActions.jobMasterFailed(cause);
        }
    }

    private void jobStatusChanged(final JobStatus newJobStatus) {
        validateRunsInMainThread();

        if(newJobStatus.isGloballyTerminalState()) {
            runAsync(() -> registeredTaskManagers.keySet().forEach(
                    newJobStatus == JobStatus.FINISHED ? partitionTracker::stopTrackingAndReleaseOrPromotePartitionsFor : partitionTracker::stopTrackingAndReleasePartitionsFor));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 给 job 进行打包归档： ArchivedExecutionGraph
             */
            final ArchivedExecutionGraph archivedExecutionGraph = schedulerNG.requestJob();
            scheduledExecutorService
                    .execute(() -> jobCompletionActions.jobReachedGloballyTerminalState(archivedExecutionGraph));
        }
    }

    private void notifyOfNewResourceManagerLeader(final String newResourceManagerAddress,
            final ResourceManagerId resourceManagerId) {

        // TODO_MA 注释： 创建得到新的 RM 的地址
        resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重连 ResourceManager
         */
        reconnectToResourceManager(new FlinkException(
                String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(@Nullable String newResourceManagerAddress,
            @Nullable ResourceManagerId resourceManagerId) {
        if(newResourceManagerAddress != null) {
            // the contract is: address == null <=> id == null
            checkNotNull(resourceManagerId);
            return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        } else {
            return null;
        }
    }

    private void reconnectToResourceManager(Exception cause) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 关闭已存在的 JobMaster 和 ResourceManager 之间的链接
         */
        closeResourceManagerConnection(cause);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重新链接
         */
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if(resourceManagerAddress != null) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：  重新链接
             */
            connectToResourceManager();
        }
    }

    private void connectToResourceManager() {
        assert (resourceManagerAddress != null);
        assert (resourceManagerConnection == null);
        assert (establishedResourceManagerConnection == null);

        log.info("Connecting to ResourceManager {}", resourceManagerAddress);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 构建一个 ResourceManagerConnection 链接对象
         */
        resourceManagerConnection = new ResourceManagerConnection(log, jobGraph.getJobID(), resourceId, getAddress(),
                getFencingToken(), resourceManagerAddress.getAddress(), resourceManagerAddress.getResourceManagerId(),
                scheduledExecutorService);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： JobMaster 去联系 ResourceManager
         */
        resourceManagerConnection.start();
    }

    private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
        final ResourceManagerId resourceManagerId = success.getResourceManagerId();

        // verify the response with current connection
        if(resourceManagerConnection != null && Objects
                .equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

            log.info("JobManager successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

            // TODO_MA 注释： 获取 Gateway
            final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

            final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

            // TODO_MA 注释： 构建链接对象
            // TODO_MA 注释： 如果说一个组件 完成了另外一个组件的链接
            // TODO_MA 注释： 事实上，就是获取到了对方的一个 Gateway 然后保存在链接对象内部
            // TODO_MA 注释： 如果现在需要跟对方取得联系，直接调用 Gateway 发送 RPC 请求即可
            establishedResourceManagerConnection = new EstablishedResourceManagerConnection(resourceManagerGateway,
                    resourceManagerResourceId);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 申请资源
             *  如果现在这个 JobMaster 是从异常 job 中恢复的。
             *  那么极有可能已经存在了一些申请 slot 的请求，还没有处理
             *  那么这些请求，在 新的 JobMaster 启动好了之后，完成跟 ResourceManager 的链接和注册之后，这些请求就可以去处理了
             */
            slotPool.connectToResourceManager(resourceManagerGateway);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 管理心跳
             */
            resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<Void>() {
                @Override
                public void receiveHeartbeat(ResourceID resourceID, Void payload) {

                    // TODO_MA 注释： jobmaster 给 resourcemanager 发送心跳反馈
                    resourceManagerGateway.heartbeatFromJobManager(resourceID);
                }

                @Override
                public void requestHeartbeat(ResourceID resourceID, Void payload) {
                    // request heartbeat will never be called on the job manager side
                }
            });
        } else {
            log.debug("Ignoring resource manager connection to {} because it's duplicated or outdated.",
                    resourceManagerId);
        }
    }

    private void closeResourceManagerConnection(Exception cause) {
        if(establishedResourceManagerConnection != null) {
            dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
            establishedResourceManagerConnection = null;
        }

        if(resourceManagerConnection != null) {
            // stop a potentially ongoing registration process
            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void dissolveResourceManagerConnection(
            EstablishedResourceManagerConnection establishedResourceManagerConnection, Exception cause) {
        final ResourceID resourceManagerResourceID = establishedResourceManagerConnection.getResourceManagerResourceID();

        if(log.isDebugEnabled()) {
            log.debug("Close ResourceManager connection {}.", resourceManagerResourceID.getStringWithMetadata(), cause);
        } else {
            log.info("Close ResourceManager connection {}: {}.", resourceManagerResourceID.getStringWithMetadata(),
                    cause.getMessage());
        }

        resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

        ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
        resourceManagerGateway.disconnectJobManager(jobGraph.getJobID(), cause);
        slotPool.disconnectResourceManager();
    }

    // ----------------------------------------------------------------------------------------------
    // Service methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public JobMasterGateway getGateway() {
        return getSelfGateway(JobMasterGateway.class);
    }

    // ----------------------------------------------------------------------------------------------
    // Utility classes
    // ----------------------------------------------------------------------------------------------

    private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 监听回调
             */
            runAsync(() -> notifyOfNewResourceManagerLeader(leaderAddress,
                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(final Exception exception) {
            handleJobMasterError(new Exception("Fatal error in the ResourceManager leader service", exception));
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class ResourceManagerConnection extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> {
        private final JobID jobID;

        private final ResourceID jobManagerResourceID;

        private final String jobManagerRpcAddress;

        private final JobMasterId jobMasterId;

        ResourceManagerConnection(final Logger log, final JobID jobID, final ResourceID jobManagerResourceID,
                final String jobManagerRpcAddress, final JobMasterId jobMasterId, final String resourceManagerAddress,
                final ResourceManagerId resourceManagerId, final Executor executor) {
            super(log, resourceManagerAddress, resourceManagerId, executor);
            this.jobID = checkNotNull(jobID);
            this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
            this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
            this.jobMasterId = checkNotNull(jobMasterId);
        }

        @Override
        protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 生成一个注册对象
             */
            return new RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess>(log,
                    getRpcService(), "ResourceManager", ResourceManagerGateway.class, getTargetAddress(),
                    getTargetLeaderId(), jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 注册
                 */
                @Override
                protected CompletableFuture<RegistrationResponse> invokeRegistration(ResourceManagerGateway gateway,
                        ResourceManagerId fencingToken, long timeoutMillis) {
                    Time timeout = Time.milliseconds(timeoutMillis);

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： JobMaster 向 ResourceManager 注册！
                     *  １２３
                     */
                    return gateway
                            .registerJobManager(jobMasterId, jobManagerResourceID, jobManagerRpcAddress, jobID, timeout);
                }
            };
        }

        @Override
        protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
            runAsync(() -> {
                // filter out outdated connections
                //noinspection ObjectEquality
                if(this == resourceManagerConnection) {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： JobMaster 完成和 ResourceManager 之间的链接
                     */
                    establishResourceManagerConnection(success);
                }
            });
        }

        @Override
        protected void onRegistrationFailure(final Throwable failure) {
            handleJobMasterError(failure);
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class JobManagerJobStatusListener implements JobStatusListener {

        private volatile boolean running = true;

        @Override
        public void jobStatusChanges(final JobID jobId, final JobStatus newJobStatus, final long timestamp,
                final Throwable error) {

            if(running) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： Job 的状态发生改变，则该方法会进行回调
                 */
                // run in rpc thread to avoid concurrency
                runAsync(() -> jobStatusChanged(newJobStatus));
            }
        }

        private void stop() {
            running = false;
        }
    }

    private class TaskManagerHeartbeatListener implements HeartbeatListener<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> {

        @Override
        public void notifyHeartbeatTimeout(ResourceID resourceID) {
            validateRunsInMainThread();
            disconnectTaskManager(resourceID, new TimeoutException(
                    "Heartbeat of TaskManager with id " + resourceID.getStringWithMetadata() + " timed out."));
        }

        @Override
        public void reportPayload(ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
            validateRunsInMainThread();
            executionDeploymentReconciler
                    .reconcileExecutionDeployments(resourceID, payload.getExecutionDeploymentReport(),
                            executionDeploymentTracker.getExecutionsOn(resourceID));
            for(AccumulatorSnapshot snapshot : payload.getAccumulatorReport().getAccumulatorSnapshots()) {
                schedulerNG.updateAccumulators(snapshot);
            }
        }

        @Override
        public AllocatedSlotReport retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return slotPool.createAllocatedSlotReport(resourceID);
        }
    }

    private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            validateRunsInMainThread();
            log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId.getStringWithMetadata());

            if(establishedResourceManagerConnection != null && establishedResourceManagerConnection
                    .getResourceManagerResourceID().equals(resourceId)) {
                reconnectToResourceManager(new JobMasterException(
                        String.format("The heartbeat of ResourceManager with id %s timed out.",
                                resourceId.getStringWithMetadata())));
            }
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }
}
