/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The future default scheduler.
 */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

    private final Logger log;

    private final ClassLoader userCodeLoader;

    private final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionFailureHandler executionFailureHandler;

    private final ScheduledExecutor delayExecutor;

    private final SchedulingStrategy schedulingStrategy;

    private final ExecutionVertexOperations executionVertexOperations;

    private final Set<ExecutionVertexID> verticesWaitingForRestart;

    private final Consumer<ComponentMainThreadExecutor> startUpAction;

    DefaultScheduler(final Logger log, final JobGraph jobGraph, final BackPressureStatsTracker backPressureStatsTracker,
            final Executor ioExecutor, final Configuration jobMasterConfiguration,
            final Consumer<ComponentMainThreadExecutor> startUpAction, final ScheduledExecutorService futureExecutor,
            final ScheduledExecutor delayExecutor, final ClassLoader userCodeLoader,
            final CheckpointRecoveryFactory checkpointRecoveryFactory, final Time rpcTimeout, final BlobWriter blobWriter,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup, final ShuffleMaster<?> shuffleMaster,
            final JobMasterPartitionTracker partitionTracker, final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ExecutionVertexOperations executionVertexOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
            final ExecutionDeploymentTracker executionDeploymentTracker, long initializationTimestamp) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 调用父类构造， 在父类构造实现中，会生成 ExecutionGraph
         */
        super(log, jobGraph, backPressureStatsTracker, ioExecutor, jobMasterConfiguration, new ThrowingSlotProvider(),
                // this is not used any more in the new scheduler
                futureExecutor, userCodeLoader, checkpointRecoveryFactory, rpcTimeout,
                new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(), blobWriter, jobManagerJobMetricGroup,
                Time.seconds(0),
                // this is not used any more in the new scheduler
                shuffleMaster, partitionTracker, executionVertexVersioner, executionDeploymentTracker, false,
                initializationTimestamp);

        this.log = log;

        this.delayExecutor = checkNotNull(delayExecutor);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.executionVertexOperations = checkNotNull(executionVertexOperations);

        final FailoverStrategy failoverStrategy = failoverStrategyFactory
                .create(getSchedulingTopology(), getResultPartitionAvailabilityChecker());
        log.info("Using failover strategy {} for {} ({}).", failoverStrategy, jobGraph.getName(), jobGraph.getJobID());

        this.executionFailureHandler = new ExecutionFailureHandler(getSchedulingTopology(), failoverStrategy,
                restartBackoffTimeStrategy);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： PipelinedRegionSchedulingStrategy
         */
        this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： SlotSharingExecutionSlotAllocator
         */
        this.executionSlotAllocator = checkNotNull(executionSlotAllocatorFactory)
                .createInstance(new DefaultExecutionSlotAllocationContext());

        this.verticesWaitingForRestart = new HashSet<>();
        this.startUpAction = startUpAction;
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    @Override
    public void setMainThreadExecutor(ComponentMainThreadExecutor mainThreadExecutor) {
        super.setMainThreadExecutor(mainThreadExecutor);
        startUpAction.accept(mainThreadExecutor);
    }

    @Override
    protected long getNumberOfRestarts() {
        return executionFailureHandler.getNumberOfRestarts();
    }

    @Override
    protected void startSchedulingInternal() {
        log.info("Starting scheduling with scheduling strategy [{}]", schedulingStrategy.getClass().getName());

        // TODO_MA 注释： ExecutionGraph 的初始状态： CREATED
        // TODO_MA 注释： 更改 ExecutionGraph 的状态为： RUNNING
        prepareExecutionGraphForNgScheduling();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 通过 SchedulingStrategy 来调度
         *  Flink-1.12 版本改为：PipelinedRegionSchedulingStrategy 来做实现
         *  调度策略！
         */
        schedulingStrategy.startScheduling();
    }

    @Override
    protected void updateTaskExecutionStateInternal(final ExecutionVertexID executionVertexId,
            final TaskExecutionStateTransition taskExecutionState) {

        schedulingStrategy.onExecutionStateChange(executionVertexId, taskExecutionState.getExecutionState());
        maybeHandleTaskFailure(taskExecutionState, executionVertexId);
    }

    private void maybeHandleTaskFailure(final TaskExecutionStateTransition taskExecutionState,
            final ExecutionVertexID executionVertexId) {

        if(taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
            final Throwable error = taskExecutionState.getError(userCodeLoader);
            handleTaskFailure(executionVertexId, error);
        }
    }

    private void handleTaskFailure(final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
        setGlobalFailureCause(error);
        notifyCoordinatorsAboutTaskFailure(executionVertexId, error);
        final FailureHandlingResult failureHandlingResult = executionFailureHandler
                .getFailureHandlingResult(executionVertexId, error);
        maybeRestartTasks(failureHandlingResult);
    }

    private void notifyCoordinatorsAboutTaskFailure(final ExecutionVertexID executionVertexId,
            @Nullable final Throwable error) {
        final ExecutionJobVertex jobVertex = getExecutionJobVertex(executionVertexId.getJobVertexId());
        final int subtaskIndex = executionVertexId.getSubtaskIndex();

        jobVertex.getOperatorCoordinators().forEach(c -> c.subtaskFailed(subtaskIndex, error));
    }

    @Override
    public void handleGlobalFailure(final Throwable error) {
        setGlobalFailureCause(error);

        log.info("Trying to recover from a global failure.", error);
        final FailureHandlingResult failureHandlingResult = executionFailureHandler.getGlobalFailureHandlingResult(error);
        maybeRestartTasks(failureHandlingResult);
    }

    private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
        if(failureHandlingResult.canRestart()) {
            restartTasksWithDelay(failureHandlingResult);
        } else {
            failJob(failureHandlingResult.getError());
        }
    }

    private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
        final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();

        final Set<ExecutionVertexVersion> executionVertexVersions = new HashSet<>(
                executionVertexVersioner.recordVertexModifications(verticesToRestart).values());
        final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

        addVerticesToRestartPending(verticesToRestart);

        final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

        delayExecutor.schedule(() -> FutureUtils.assertNoException(cancelFuture
                        .thenRunAsync(restartTasks(executionVertexVersions, globalRecovery), getMainThreadExecutor())),
                failureHandlingResult.getRestartDelayMS(), TimeUnit.MILLISECONDS);
    }

    private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.addAll(verticesToRestart);
        transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
    }

    private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
        verticesWaitingForRestart.removeAll(verticesToRestart);
        if(verticesWaitingForRestart.isEmpty()) {
            transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
        }
    }

    private Runnable restartTasks(final Set<ExecutionVertexVersion> executionVertexVersions,
            final boolean isGlobalRecovery) {
        return () -> {
            final Set<ExecutionVertexID> verticesToRestart = executionVertexVersioner
                    .getUnmodifiedExecutionVertices(executionVertexVersions);

            removeVerticesFromRestartPending(verticesToRestart);

            resetForNewExecutions(verticesToRestart);

            try {
                restoreState(verticesToRestart, isGlobalRecovery);
            } catch(Throwable t) {
                handleGlobalFailure(t);
                return;
            }

            schedulingStrategy.restartTasks(verticesToRestart);
        };
    }

    private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
        final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream().map(this::cancelExecutionVertex)
                .collect(Collectors.toList());

        return FutureUtils.combineAll(cancelFutures);
    }

    private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
        final ExecutionVertex vertex = getExecutionVertex(executionVertexId);

        notifyCoordinatorOfCancellation(vertex);

        executionSlotAllocator.cancel(executionVertexId);
        return executionVertexOperations.cancel(vertex);
    }

    @Override
    protected void scheduleOrUpdateConsumersInternal(final IntermediateResultPartitionID partitionId) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        schedulingStrategy.onPartitionConsumable(partitionId);
    }

    // ------------------------------------------------------------------------
    // SchedulerOperations
    // ------------------------------------------------------------------------

    @Override
    public void allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
        validateDeploymentOptions(executionVertexDeploymentOptions);

        final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex = groupDeploymentOptionsByVertexId(
                executionVertexDeploymentOptions);

        final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
                .map(ExecutionVertexDeploymentOption::getExecutionVertexId).collect(Collectors.toList());

        final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex = executionVertexVersioner
                .recordVertexModifications(verticesToDeploy);

        transitionToScheduled(verticesToDeploy);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 申请 Slot
         *  1、Slot 会明确的表示出来，该 slot 是属于哪个 TaskExecutor 中的第几个 slot
         *  2、ExecutionVertex 代表是 ExecutionGraph　中一个执行顶点
         *  ExecutionGraph 中的任何一个 执行顶点，都和申请到的所有 slots 中的某一个 slot 产生了映射关系
         *  ExecutionVertex ===> Slot ==> TaskExecutor  ==> Index
         *  -
         *  SlotExecutionVertexAssignment描述的是一个 顶点Task 和 一个 Slot 的分派关系的映射
         */
        final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = allocateSlots(
                executionVertexDeploymentOptions);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 把每一个 SlotExecutionVertexAssignment 转变成 DeploymentHandle
         *  -
         *  SlotExecutionVertexAssignment ==> DeploymentHandle
         */
        final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(requiredVersionByVertex,
                deploymentOptionsByVertex, slotExecutionVertexAssignments);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 部署 Task
         *  通过 DeploymentHandle 来部署 ExecutionVertex 顶点所对应的 Task
         */
        waitForAllSlotsAndDeploy(deploymentHandles);
    }

    private void validateDeploymentOptions(final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
        deploymentOptions.stream().map(ExecutionVertexDeploymentOption::getExecutionVertexId)
                .map(this::getExecutionVertex).forEach(v -> checkState(v.getExecutionState() == ExecutionState.CREATED,
                "expected vertex %s to be in CREATED state, was: %s", v.getID(), v.getExecutionState()));
    }

    private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(
            final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
        return executionVertexDeploymentOptions.stream()
                .collect(Collectors.toMap(ExecutionVertexDeploymentOption::getExecutionVertexId, Function.identity()));
    }

    // TODO_MA 注释： Option : Some  None
    // TODO_MA 注释： Set<ExecutionVertexID> ==> List<ExecutionVertexDeploymentOption>
    // TODO_MA 注释： 返回结果： List<SlotExecutionVertexAssignment> 分配
    private List<SlotExecutionVertexAssignment> allocateSlots(
            final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： SlotSharingExecutionSlotAllocator
         *  -
         *  包装顺序：
         *  1、JobMaster
         *  2、Scheduler == DefaultScheduler
         *  3、SchedulerStategy
         *  4、ExecutionSlotAllocator = SlotSharingExecutionSlotAllocator
         */
        return executionSlotAllocator.allocateSlotsFor(
                executionVertexDeploymentOptions.stream().map(ExecutionVertexDeploymentOption::getExecutionVertexId)
                        .map(this::getExecutionVertex).map(ExecutionVertexSchedulingRequirementsMapper::from)
                        .collect(Collectors.toList()));
    }

    private static List<DeploymentHandle> createDeploymentHandles(
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
            final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
            final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

        return slotExecutionVertexAssignments.stream().map(slotExecutionVertexAssignment -> {
            final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 给每一个 SlotExecutionVertexAssignment 构建一个 DeploymentHandle
             */
            return new DeploymentHandle(requiredVersionByVertex.get(executionVertexId),
                    deploymentOptionsByVertex.get(executionVertexId), slotExecutionVertexAssignment);
        }).collect(Collectors.toList());
    }

    private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 部署 Task
         *  1、assignAllResources(deploymentHandles) 进行资源分配：Execution 和 LogicalSlot 联系在一起
         *      当这个方法执行，每个Task 到底要被部署到那个从节点上去运行
         *      LogicalSlot(jobid, slotid, allocationid, taskexecutorGateway)
         *      我需要把这个 job 中的第几个 Task 分配到那个从节点中的那个 slot 去运行
         *  2、deployAll(deploymentHandles) 执行部署
         *      完成所有Task的部署
         */
        FutureUtils.assertNoException(assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles)));
    }

    private CompletableFuture<Void> assignAllResources(final List<DeploymentHandle> deploymentHandles) {
        final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
        for(DeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<Void> slotAssigned = deploymentHandle.getSlotExecutionVertexAssignment()

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释：
                     */.getLogicalSlotFuture().handle(assignResourceOrHandleError(deploymentHandle));
            slotAssignedFutures.add(slotAssigned);
        }
        return FutureUtils.waitForAll(slotAssignedFutures);
    }

    private BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);

            // TODO_MA 注释： 遍历部署每一个 Task
            for(final DeploymentHandle deploymentHandle : deploymentHandles) {

                // TODO_MA 注释： 获取 Slot 和 Vertex 的映射关系
                final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle
                        .getSlotExecutionVertexAssignment();
                final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                FutureUtils.assertNoException(slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }
            return null;
        };
    }

    private static void propagateIfNonNull(final Throwable throwable) {
        if(throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(
            final DeploymentHandle deploymentHandle) {
        final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
        final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

        return (logicalSlot, throwable) -> {
            if(executionVertexVersioner.isModified(requiredVertexVersion)) {
                log.debug(
                        "Refusing to assign slot to execution vertex {} because this deployment was " + "superseded by another deployment",
                        executionVertexId);
                releaseSlotIfPresent(logicalSlot);
                return null;
            }

            if(throwable == null) {
                final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
                final boolean sendScheduleOrUpdateConsumerMessage = deploymentHandle.getDeploymentOption()
                        .sendScheduleOrUpdateConsumerMessage();
                executionVertex.getCurrentExecutionAttempt()
                        .registerProducedPartitions(logicalSlot.getTaskManagerLocation(),
                                sendScheduleOrUpdateConsumerMessage);

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                executionVertex.tryAssignResource(logicalSlot);
            } else {
                handleTaskDeploymentFailure(executionVertexId, maybeWrapWithNoResourceAvailableException(throwable));
            }
            return null;
        };
    }

    private void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if(logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private void handleTaskDeploymentFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
        executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if(strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. " + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    private BiFunction<Object, Throwable, Void> deployOrHandleError(final DeploymentHandle deploymentHandle) {
        final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
        final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

        return (ignored, throwable) -> {
            if(executionVertexVersioner.isModified(requiredVertexVersion)) {
                log.debug(
                        "Refusing to deploy execution vertex {} because this deployment was " + "superseded by another deployment",
                        executionVertexId);
                return null;
            }

            if(throwable == null) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                deployTaskSafe(executionVertexId);
            } else {
                handleTaskDeploymentFailure(executionVertexId, throwable);
            }
            return null;
        };
    }

    private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
        try {

            // TODO_MA 注释： 根据 executionVertexId 获取到 ExecutionVertex
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 部署一个 ExecutionVertex
             */
            executionVertexOperations.deploy(executionVertex);

        } catch(Throwable e) {
            handleTaskDeploymentFailure(executionVertexId, e);
        }
    }

    private void notifyCoordinatorOfCancellation(ExecutionVertex vertex) {
        // this method makes a best effort to filter out duplicate notifications, meaning cases
        // where
        // the coordinator was already notified for that specific task
        // we don't notify if the task is already FAILED, CANCELLING, or CANCELED

        final ExecutionState currentState = vertex.getExecutionState();
        if(currentState == ExecutionState.FAILED || currentState == ExecutionState.CANCELING || currentState == ExecutionState.CANCELED) {
            return;
        }

        for(OperatorCoordinator coordinator : vertex.getJobVertex().getOperatorCoordinators()) {
            coordinator.subtaskFailed(vertex.getParallelSubtaskIndex(), null);
        }
    }

    private class DefaultExecutionSlotAllocationContext implements ExecutionSlotAllocationContext {

        @Override
        public ResourceProfile getResourceProfile(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getResourceProfile();
        }

        @Override
        public AllocationID getPriorAllocationId(final ExecutionVertexID executionVertexId) {
            return getExecutionVertex(executionVertexId).getLatestPriorAllocation();
        }

        @Override
        public SchedulingTopology getSchedulingTopology() {
            return DefaultScheduler.this.getSchedulingTopology();
        }

        @Override
        public Set<SlotSharingGroup> getLogicalSlotSharingGroups() {
            return getJobGraph().getSlotSharingGroups();
        }

        @Override
        public Set<CoLocationGroupDesc> getCoLocationGroups() {
            return getJobGraph().getCoLocationGroupDescriptors();
        }

        @Override
        public Collection<Collection<ExecutionVertexID>> getConsumedResultPartitionsProducers(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getConsumedResultPartitionsProducers(executionVertexId);
        }

        @Override
        public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
                ExecutionVertexID executionVertexId) {
            return inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);
        }

        @Override
        public Optional<TaskManagerLocation> getStateLocation(ExecutionVertexID executionVertexId) {
            return stateLocationRetriever.getStateLocation(executionVertexId);
        }
    }
}
