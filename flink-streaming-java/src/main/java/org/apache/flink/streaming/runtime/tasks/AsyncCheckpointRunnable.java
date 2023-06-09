/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This runnable executes the asynchronous parts of all involved backend snapshots for the subtask.
 */
final class AsyncCheckpointRunnable implements Runnable, Closeable {

    public static final Logger LOG = LoggerFactory.getLogger(AsyncCheckpointRunnable.class);
    private final String taskName;
    private final Consumer<AsyncCheckpointRunnable> registerConsumer;
    private final Consumer<AsyncCheckpointRunnable> unregisterConsumer;
    private final Environment taskEnvironment;

    public boolean isRunning() {
        return asyncCheckpointState.get() == AsyncCheckpointState.RUNNING;
    }

    enum AsyncCheckpointState {
        RUNNING, DISCARDED, COMPLETED
    }

    private final AsyncExceptionHandler asyncExceptionHandler;
    private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;
    private final CheckpointMetaData checkpointMetaData;
    private final CheckpointMetricsBuilder checkpointMetrics;
    private final long asyncConstructionNanos;
    private final AtomicReference<AsyncCheckpointState> asyncCheckpointState = new AtomicReference<>(
            AsyncCheckpointState.RUNNING);

    AsyncCheckpointRunnable(Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData, CheckpointMetricsBuilder checkpointMetrics,
            long asyncConstructionNanos, String taskName, Consumer<AsyncCheckpointRunnable> register,
            Consumer<AsyncCheckpointRunnable> unregister, Environment taskEnvironment,
            AsyncExceptionHandler asyncExceptionHandler) {

        this.operatorSnapshotsInProgress = checkNotNull(operatorSnapshotsInProgress);
        this.checkpointMetaData = checkNotNull(checkpointMetaData);
        this.checkpointMetrics = checkNotNull(checkpointMetrics);
        this.asyncConstructionNanos = asyncConstructionNanos;
        this.taskName = checkNotNull(taskName);
        this.registerConsumer = register;
        this.unregisterConsumer = unregister;
        this.taskEnvironment = checkNotNull(taskEnvironment);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
    }

    @Override
    public void run() {
        final long asyncStartNanos = System.nanoTime();
        final long asyncStartDelayMillis = (asyncStartNanos - asyncConstructionNanos) / 1_000_000L;
        LOG.debug("{} - started executing asynchronous part of checkpoint {}. Asynchronous start delay: {} ms", taskName,
                checkpointMetaData.getCheckpointId(), asyncStartDelayMillis);

        FileSystemSafetyNet.initializeSafetyNetForThread();
        try {

            registerConsumer.accept(this);

            TaskStateSnapshot jobManagerTaskOperatorSubtaskStates = new TaskStateSnapshot(
                    operatorSnapshotsInProgress.size());
            TaskStateSnapshot localTaskOperatorSubtaskStates = new TaskStateSnapshot(operatorSnapshotsInProgress.size());

            long bytesPersistedDuringAlignment = 0;
            for(Map.Entry<OperatorID, OperatorSnapshotFutures> entry : operatorSnapshotsInProgress.entrySet()) {

                OperatorID operatorID = entry.getKey();
                OperatorSnapshotFutures snapshotInProgress = entry.getValue();

                // finalize the async part of all by executing all snapshot runnables
                OperatorSnapshotFinalizer finalizedSnapshots = new OperatorSnapshotFinalizer(snapshotInProgress);

                // TODO_MA 注释： 保存 state
                jobManagerTaskOperatorSubtaskStates
                        .putSubtaskStateByOperatorID(operatorID, finalizedSnapshots.getJobManagerOwnedState());

                localTaskOperatorSubtaskStates
                        .putSubtaskStateByOperatorID(operatorID, finalizedSnapshots.getTaskLocalState());

                bytesPersistedDuringAlignment += finalizedSnapshots.getJobManagerOwnedState().getResultSubpartitionState()
                        .getStateSize();
                bytesPersistedDuringAlignment += finalizedSnapshots.getJobManagerOwnedState().getInputChannelState()
                        .getStateSize();
            }

            final long asyncEndNanos = System.nanoTime();
            final long asyncDurationMillis = (asyncEndNanos - asyncConstructionNanos) / 1_000_000L;

            checkpointMetrics.setBytesPersistedDuringAlignment(bytesPersistedDuringAlignment);
            checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 先比对状态，如果是 COMPLETED 则汇报
             */
            if(asyncCheckpointState.compareAndSet(AsyncCheckpointState.RUNNING, AsyncCheckpointState.COMPLETED)) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 汇报
                 */
                reportCompletedSnapshotStates(jobManagerTaskOperatorSubtaskStates, localTaskOperatorSubtaskStates,
                        asyncDurationMillis);
            } else {
                LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
                        taskName, checkpointMetaData.getCheckpointId());
            }
        } catch(Exception e) {
            LOG.info("{} - asynchronous part of checkpoint {} could not be completed.", taskName,
                    checkpointMetaData.getCheckpointId(), e);
            handleExecutionException(e);
        } finally {
            unregisterConsumer.accept(this);
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }
    }

    private void reportCompletedSnapshotStates(TaskStateSnapshot acknowledgedTaskStateSnapshot,
            TaskStateSnapshot localTaskStateSnapshot, long asyncDurationMillis) {

        boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
        boolean hasLocalState = localTaskStateSnapshot.hasState();

        checkState(hasAckState || !hasLocalState,
                "Found cached state but no corresponding primary state is reported to the job " + "manager. This indicates a problem.");

        // we signal stateless tasks by reporting null, so that there are no attempts to assign
        // empty state
        // to stateless tasks on restore. This enables simple job modifications that only concern
        // stateless without the need to assign them uids to match their (always empty) states.

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 通过 TaskStateManager 汇报 Task 的 state snapshot 状态给 CheckpointCoordinator
         */
        taskEnvironment.getTaskStateManager().reportTaskStateSnapshots(checkpointMetaData, checkpointMetrics.build(),
                hasAckState ? acknowledgedTaskStateSnapshot : null, hasLocalState ? localTaskStateSnapshot : null);

        LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms", taskName,
                checkpointMetaData.getCheckpointId(), asyncDurationMillis);

        LOG.trace("{} - reported the following states in snapshot for checkpoint {}: {}.", taskName,
                checkpointMetaData.getCheckpointId(), acknowledgedTaskStateSnapshot);
    }

    private void handleExecutionException(Exception e) {

        boolean didCleanup = false;
        AsyncCheckpointState currentState = asyncCheckpointState.get();

        while(AsyncCheckpointState.DISCARDED != currentState) {

            if(asyncCheckpointState.compareAndSet(currentState, AsyncCheckpointState.DISCARDED)) {

                didCleanup = true;

                try {
                    cleanup();
                } catch(Exception cleanupException) {
                    e.addSuppressed(cleanupException);
                }

                Exception checkpointException = new Exception("Could not materialize checkpoint " + checkpointMetaData
                        .getCheckpointId() + " for operator " + taskName + '.', e);

                // We only report the exception for the original cause of fail and cleanup.
                // Otherwise this followup exception could race the original exception in failing
                // the task.
                try {
                    taskEnvironment.declineCheckpoint(checkpointMetaData.getCheckpointId(),
                            new CheckpointException(CheckpointFailureReason.CHECKPOINT_ASYNC_EXCEPTION,
                                    checkpointException));
                } catch(Exception unhandled) {
                    AsynchronousException asyncException = new AsynchronousException(unhandled);
                    asyncExceptionHandler
                            .handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
                }

                currentState = AsyncCheckpointState.DISCARDED;
            } else {
                currentState = asyncCheckpointState.get();
            }
        }

        if(!didCleanup) {
            LOG.trace("Caught followup exception from a failed checkpoint thread. This can be ignored.", e);
        }
    }

    @Override
    public void close() {
        if(asyncCheckpointState.compareAndSet(AsyncCheckpointState.RUNNING, AsyncCheckpointState.DISCARDED)) {

            try {
                cleanup();
            } catch(Exception cleanupException) {
                LOG.warn("Could not properly clean up the async checkpoint runnable.", cleanupException);
            }
        } else {
            logFailedCleanupAttempt();
        }
    }

    long getCheckpointId() {
        return checkpointMetaData.getCheckpointId();
    }

    private void cleanup() throws Exception {
        LOG.debug("Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.", checkpointMetaData.getCheckpointId(),
                taskName);

        Exception exception = null;

        // clean up ongoing operator snapshot results and non partitioned state handles
        for(OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
            if(operatorSnapshotResult != null) {
                try {
                    operatorSnapshotResult.cancel();
                } catch(Exception cancelException) {
                    exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
                }
            }
        }

        if(null != exception) {
            throw exception;
        }
    }

    private void logFailedCleanupAttempt() {
        LOG.debug(
                "{} - asynchronous checkpointing operation for checkpoint {} has " + "already been completed. Thus, the state handles are not cleaned up.",
                taskName, checkpointMetaData.getCheckpointId());
    }
}
