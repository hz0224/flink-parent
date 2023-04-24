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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnfulfillableSlotRequestException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link SlotManager}.
 */
public class SlotManagerImpl implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(SlotManagerImpl.class);

    /**
     * Scheduled executor for timeouts.
     */
    private final ScheduledExecutor scheduledExecutor;

    /**
     * Timeout for slot requests to the task manager.
     */
    private final Time taskManagerRequestTimeout;

    /**
     * Timeout after which an allocation is discarded.
     */
    private final Time slotRequestTimeout;

    /**
     * Timeout after which an unused TaskManager is released.
     */
    private final Time taskManagerTimeout;

    /**
     * Map for all registered slots.
     */
    private final HashMap<SlotID, TaskManagerSlot> slots;

    /**
     * Index of all currently free slots.
     */
    private final LinkedHashMap<SlotID, TaskManagerSlot> freeSlots;

    /**
     * All currently registered task managers.
     */
    private final HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations;

    /**
     * Map of fulfilled and active allocations for request deduplication purposes.
     */
    private final HashMap<AllocationID, SlotID> fulfilledSlotRequests;

    /**
     * Map of pending/unfulfilled slot allocation requests.
     */
    private final HashMap<AllocationID, PendingSlotRequest> pendingSlotRequests;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 资源不足的时候会通过 ResourceActions#allocateResource(ResourceProfile)
     *  申请新的资源（可能启动新的 TaskManager，也可能什么也不做），
     * 	这些新申请的资源会被封装为 PendingTaskManagerSlot
     */
    private final HashMap<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots;

    private final SlotMatchingStrategy slotMatchingStrategy;

    /**
     * ResourceManager's id.
     */
    private ResourceManagerId resourceManagerId;

    /**
     * Executor for future callbacks which have to be "synchronized".
     */
    private Executor mainThreadExecutor;

    /**
     * Callbacks for resource (de-)allocations.
     */
    private ResourceActions resourceActions;

    private ScheduledFuture<?> taskManagerTimeoutsAndRedundancyCheck;

    private ScheduledFuture<?> slotRequestTimeoutCheck;

    /**
     * True iff the component has been started.
     */
    private boolean started;

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    /**
     * Defines the max limitation of the total number of slots.
     */
    private final int maxSlotNum;

    /**
     * Defines the number of redundant taskmanagers.
     */
    private final int redundantTaskManagerNum;

    /**
     * If true, fail unfulfillable slot requests immediately. Otherwise, allow unfulfillable request
     * to pend. A slot request is considered unfulfillable if it cannot be fulfilled by neither a
     * slot that is already registered (including allocated ones) nor a pending slot that the {@link
     * ResourceActions} can allocate.
     */
    private boolean failUnfulfillableRequest = true;

    /**
     * The default resource spec of workers to request.
     */
    private final WorkerResourceSpec defaultWorkerResourceSpec;

    private final int numSlotsPerWorker;

    private final ResourceProfile defaultSlotResourceProfile;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 初始化一大堆组件，都是用来帮助 ResourceManager 管理 Slot 资源的
     */
    public SlotManagerImpl(ScheduledExecutor scheduledExecutor, SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();
        this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
        this.slotRequestTimeout = slotManagerConfiguration.getSlotRequestTimeout();
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.waitResultConsumedBeforeRelease = slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.defaultWorkerResourceSpec = slotManagerConfiguration.getDefaultWorkerResourceSpec();
        this.numSlotsPerWorker = slotManagerConfiguration.getNumSlotsPerWorker();
        this.defaultSlotResourceProfile = generateDefaultSlotResourceProfile(defaultWorkerResourceSpec,
                numSlotsPerWorker);
        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
        this.maxSlotNum = slotManagerConfiguration.getMaxSlotNum();
        this.redundantTaskManagerNum = slotManagerConfiguration.getRedundantTaskManagerNum();

        slots = new HashMap<>(16);
        freeSlots = new LinkedHashMap<>(16);
        taskManagerRegistrations = new HashMap<>(4);
        fulfilledSlotRequests = new HashMap<>(16);
        pendingSlotRequests = new HashMap<>(16);
        pendingSlots = new HashMap<>(16);

        resourceManagerId = null;
        resourceActions = null;
        mainThreadExecutor = null;
        taskManagerTimeoutsAndRedundancyCheck = null;
        slotRequestTimeoutCheck = null;

        started = false;
    }

    @Override
    public int getNumberRegisteredSlots() {
        return slots.size();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if(taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberRegisteredSlots();
        } else {
            return 0;
        }
    }

    @Override
    public int getNumberFreeSlots() {
        return freeSlots.size();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if(taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberFreeSlots();
        } else {
            return 0;
        }
    }

    @Override
    public Map<WorkerResourceSpec, Integer> getRequiredResources() {
        final int pendingWorkerNum = MathUtils.divideRoundUp(pendingSlots.size(), numSlotsPerWorker);
        return pendingWorkerNum > 0 ? Collections
                .singletonMap(defaultWorkerResourceSpec, pendingWorkerNum) : Collections.emptyMap();
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return getResourceFromNumSlots(getNumberRegisteredSlots());
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return getResourceFromNumSlots(getNumberRegisteredSlotsOf(instanceID));
    }

    @Override
    public ResourceProfile getFreeResource() {
        return getResourceFromNumSlots(getNumberFreeSlots());
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return getResourceFromNumSlots(getNumberFreeSlotsOf(instanceID));
    }

    private ResourceProfile getResourceFromNumSlots(int numSlots) {
        if(numSlots < 0 || defaultSlotResourceProfile == null) {
            return ResourceProfile.UNKNOWN;
        } else {
            return defaultSlotResourceProfile.multiply(numSlots);
        }
    }

    @VisibleForTesting
    public int getNumberPendingTaskManagerSlots() {
        return pendingSlots.size();
    }

    @Override
    public int getNumberPendingSlotRequests() {
        return pendingSlotRequests.size();
    }

    @VisibleForTesting
    public int getNumberAssignedPendingTaskManagerSlots() {
        return (int) pendingSlots.values().stream().filter(slot -> slot.getAssignedPendingSlotRequest() != null)
                .count();
    }

    // ---------------------------------------------------------------------------------------------
    // Component lifecycle methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId  to use for communication with the task managers
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceActions    to use for resource (de-)allocations
     */
    @Override
    public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor,
            ResourceActions newResourceActions) {
        LOG.info("Starting the SlotManager.");

        this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceActions = Preconditions.checkNotNull(newResourceActions);

        started = true;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 定时任务： checkTaskManagerTimeoutsAndRedundancy()，检查 TaskExecutor 是否长时间处于 idle 状态
         *  每隔 30s 执行一次
         *  心跳间隔时间 是 10s， 心跳超时时间是：50s， 检查从节点是否超时的时间：30s
         */
        taskManagerTimeoutsAndRedundancyCheck = scheduledExecutor
                .scheduleWithFixedDelay(() -> mainThreadExecutor.execute(() -> checkTaskManagerTimeoutsAndRedundancy()),
                        0L, taskManagerTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 定时任务： checkSlotRequestTimeouts()，检查 slot request 是否超时
         *  每隔 5min 执行一次
         *  SlotRequest ==> 处于申请中，则这个 SotRequest  ==> PendingRequest
         */
        slotRequestTimeoutCheck = scheduledExecutor
                .scheduleWithFixedDelay(() -> mainThreadExecutor.execute(() -> checkSlotRequestTimeouts()), 0L,
                        slotRequestTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

        registerSlotManagerMetrics();
    }

    private void registerSlotManagerMetrics() {
        slotManagerMetricGroup.gauge(MetricNames.TASK_SLOTS_AVAILABLE, () -> (long) getNumberFreeSlots());
        slotManagerMetricGroup.gauge(MetricNames.TASK_SLOTS_TOTAL, () -> (long) getNumberRegisteredSlots());
    }

    /**
     * Suspends the component. This clears the internal state of the slot manager.
     */
    @Override
    public void suspend() {
        LOG.info("Suspending the SlotManager.");

        // stop the timeout checks for the TaskManagers and the SlotRequests
        if(taskManagerTimeoutsAndRedundancyCheck != null) {
            taskManagerTimeoutsAndRedundancyCheck.cancel(false);
            taskManagerTimeoutsAndRedundancyCheck = null;
        }

        if(slotRequestTimeoutCheck != null) {
            slotRequestTimeoutCheck.cancel(false);
            slotRequestTimeoutCheck = null;
        }

        for(PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
            cancelPendingSlotRequest(pendingSlotRequest);
        }

        pendingSlotRequests.clear();

        ArrayList<InstanceID> registeredTaskManagers = new ArrayList<>(taskManagerRegistrations.keySet());

        for(InstanceID registeredTaskManager : registeredTaskManagers) {
            unregisterTaskManager(registeredTaskManager,
                    new SlotManagerException("The slot manager is being suspended."));
        }

        resourceManagerId = null;
        resourceActions = null;
        started = false;
    }

    /**
     * Closes the slot manager.
     *
     * @throws Exception if the close operation fails
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing the SlotManager.");

        suspend();
        slotManagerMetricGroup.close();
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        // no-op; don't throw an UnsupportedOperationException here because there are code paths
        // where the resource
        // manager calls this method regardless of whether declarative resource management is used
        // or not
    }

    /**
     * Requests a slot with the respective resource profile.
     *
     * @param slotRequest specifying the requested slot specs
     * @return true if the slot request was registered; false if the request is a duplicate
     * @throws ResourceManagerException if the slot request failed (e.g. not enough resources left)
     */
    @Override
    public boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException {
        checkInit();

        if(checkDuplicateRequest(slotRequest.getAllocationId())) {
            LOG.debug("Ignoring a duplicate slot request with allocation id {}.", slotRequest.getAllocationId());

            return false;
        } else {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： SlotManager 先把 SlotRequest 封装成为一个 PendingSlotRequest
             *  然后加入到 pendingSlotRequests 集合中
             *  PendingSlotRequest 意味着： 待定的 slotRequest， 未被满足要求的 slotRequest
             *  -
             *  1、在这之前，jobManager 构建了一个 PendingRequest 请求对象，并且在发送 Rpc 请求之前，也把这个对象，存储在
             *  pendingRequests 这个 map 中
             *  2、在 ResourceManager 的 slotamanager 的内部，也有一个类似的操作
             *  其实一次 slot 申请过程中，对于还没有完成 slot 申请工作，那么对于 jobamanager 是待定，对于 resourcemanager 也是待定
             */
            PendingSlotRequest pendingSlotRequest = new PendingSlotRequest(slotRequest);
            pendingSlotRequests.put(slotRequest.getAllocationId(), pendingSlotRequest);

            try {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 执行请求分配slot的逻辑
                 */
                internalRequestSlot(pendingSlotRequest);
            } catch(ResourceManagerException e) {
                // requesting the slot failed --> remove pending slot request
                pendingSlotRequests.remove(slotRequest.getAllocationId());

                throw new ResourceManagerException(
                        "Could not fulfill slot request " + slotRequest.getAllocationId() + '.', e);
            }

            return true;
        }
    }

    /**
     * Cancels and removes a pending slot request with the given allocation id. If there is no such
     * pending request, then nothing is done.
     *
     * @param allocationId identifying the pending slot request
     * @return True if a pending slot request was found; otherwise false
     */
    @Override
    public boolean unregisterSlotRequest(AllocationID allocationId) {
        checkInit();

        // TODO_MA 注释： 从 pendingSlotRequests 中移除
        PendingSlotRequest pendingSlotRequest = pendingSlotRequests.remove(allocationId);

        if(null != pendingSlotRequest) {
            LOG.debug("Cancel slot request {}.", allocationId);

            // TODO_MA 注释： 取消请求
            cancelPendingSlotRequest(pendingSlotRequest);

            return true;
        } else {
            LOG.debug("No pending slot request with allocation id {} found. Ignoring unregistration request.",
                    allocationId);

            return false;
        }
    }

    /**
     * Registers a new task manager at the slot manager. This will make the task managers slots
     * known and, thus, available for allocation.
     *
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport      for the new task manager
     * @return True if the task manager has not been registered before and is registered
     * successfully; otherwise false
     */
    @Override
    public boolean registerTaskManager(final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport) {
        checkInit();

        LOG.debug("Registering TaskManager {} under {} at the SlotManager.",
                taskExecutorConnection.getResourceID().getStringWithMetadata(), taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if(taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {

            // TODO_MA 注释： 更改状态
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return false;
        } else {
            if(isMaxSlotNumExceededAfterRegistration(initialSlotReport)) {
                LOG.info("The total number of slots exceeds the max limitation {}, release the excess resource.",
                        maxSlotNum);
                resourceActions.releaseResource(taskExecutorConnection.getInstanceID(),
                        new FlinkException("The total number of slots exceeds the max limitation."));
                return false;
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            // first register the TaskManager
            ArrayList<SlotID> reportedSlots = new ArrayList<>();
            for(SlotStatus slotStatus : initialSlotReport) {
                reportedSlots.add(slotStatus.getSlotID());
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(taskExecutorConnection,
                    reportedSlots);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);

            // next register the new slots
            for(SlotStatus slotStatus : initialSlotReport) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 注册该 TaskManager 拥有的所有 Slot Slot
                 */
                registerSlot(slotStatus.getSlotID(), slotStatus.getAllocationID(), slotStatus.getJobID(),
                        slotStatus.getResourceProfile(), taskExecutorConnection);
            }

            return true;
        }
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.debug("Unregister TaskManager {} from the SlotManager.", instanceId);

        // TODO_MA 注释： 移除
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.remove(instanceId);

        if(null != taskManagerRegistration) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            internalUnregisterTaskManager(taskManagerRegistration, cause);

            return true;
        } else {
            LOG.debug("There is no task manager registered with instance ID {}. Ignoring this message.", instanceId);
            return false;
        }
    }

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        checkInit();

        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if(null != taskManagerRegistration) {
            LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 进行 TaskExecutor 的 Slot 状态汇报
             */
            for(SlotStatus slotStatus : slotReport) {

                // TODO_MA 注释： 更改 Slot 的状态
                updateSlot(slotStatus.getSlotID(), slotStatus.getAllocationID(), slotStatus.getJobID());
            }

            return true;
        } else {
            LOG.debug("Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);

            return false;
        }
    }

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be marked as free and will be subject to new slot requests.
     *
     * @param slotId       identifying the slot to free
     * @param allocationId with which the slot is presumably allocated
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {
        checkInit();

        TaskManagerSlot slot = slots.get(slotId);

        if(null != slot) {
            if(slot.getState() == SlotState.ALLOCATED) {
                if(Objects.equals(allocationId, slot.getAllocationId())) {

                    TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations
                            .get(slot.getInstanceId());

                    if(taskManagerRegistration == null) {
                        throw new IllegalStateException("Trying to free a slot from a TaskManager " + slot
                                .getInstanceId() + " which has not been registered.");
                    }

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 更改 slot 的状态
                     */
                    updateSlotState(slot, taskManagerRegistration, null, null);
                } else {
                    LOG.debug(
                            "Received request to free slot {} with expected allocation id {}, " + "but actual allocation id {} differs. Ignoring the request.",
                            slotId, allocationId, slot.getAllocationId());
                }
            } else {
                LOG.debug("Slot {} has not been allocated.", allocationId);
            }
        } else {
            LOG.debug("Trying to free a slot {} which has not been registered. Ignoring this message.", slotId);
        }
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {

        if(!this.failUnfulfillableRequest && failUnfulfillableRequest) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 把所有的 pendingSlotRequests 拿出来进行 fail
             */
            // fail unfulfillable pending requests
            Iterator<Map.Entry<AllocationID, PendingSlotRequest>> slotRequestIterator = pendingSlotRequests.entrySet()
                    .iterator();
            while(slotRequestIterator.hasNext()) {
                PendingSlotRequest pendingSlotRequest = slotRequestIterator.next().getValue();

                // TODO_MA 注释： 申请到了的，则忽略
                if(pendingSlotRequest.getAssignedPendingTaskManagerSlot() != null) {
                    continue;
                }

                if(!isFulfillableByRegisteredOrPendingSlots(pendingSlotRequest.getResourceProfile())) {
                    slotRequestIterator.remove();

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 通知 slot 申请失败
                     */
                    resourceActions.notifyAllocationFailure(pendingSlotRequest.getJobId(),
                            pendingSlotRequest.getAllocationId(),
                            new UnfulfillableSlotRequestException(pendingSlotRequest.getAllocationId(),
                                    pendingSlotRequest.getResourceProfile()));
                }
            }
        }
        this.failUnfulfillableRequest = failUnfulfillableRequest;
    }

    // ---------------------------------------------------------------------------------------------
    // Behaviour methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Finds a matching slot request for a given resource profile. If there is no such request, the
     * method returns null.
     *
     * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
     * request fulfillment, then you should override this method.
     *
     * @param slotResourceProfile defining the resources of an available slot
     * @return A matching slot request which can be deployed in a slot with the given resource
     * profile. Null if there is no such slot request pending.
     */
    private PendingSlotRequest findMatchingRequest(ResourceProfile slotResourceProfile) {

        for(PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
            if(!pendingSlotRequest.isAssigned() && slotResourceProfile
                    .isMatching(pendingSlotRequest.getResourceProfile())) {
                return pendingSlotRequest;
            }
        }

        return null;
    }

    /**
     * Finds a matching slot for a given resource profile. A matching slot has at least as many
     * resources available as the given resource profile. If there is no such slot available, then
     * the method returns null.
     *
     * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
     * request fulfillment, then you should override this method.
     *
     * @param requestResourceProfile specifying the resource requirements for the a slot request
     * @return A matching slot which fulfills the given resource profile. {@link Optional#empty()}
     * if there is no such slot available.
     */
    private Optional<TaskManagerSlot> findMatchingSlot(ResourceProfile requestResourceProfile) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 寻找 free 状态中，符合要求的 slot
         *  默认： slotMatchingStrategy = AnyMatchingSlotMatchingStrategy
         */
        final Optional<TaskManagerSlot> optionalMatchingSlot = slotMatchingStrategy
                .findMatchingSlot(requestResourceProfile, freeSlots.values(), this::getNumberRegisteredSlotsOf);

        optionalMatchingSlot.ifPresent(taskManagerSlot -> {
            // sanity check
            Preconditions.checkState(taskManagerSlot.getState() == SlotState.FREE,
                    "TaskManagerSlot %s is not in state FREE but %s.", taskManagerSlot.getSlotId(),
                    taskManagerSlot.getState());

            // TODO_MA 注释： 从 free 集合中，移除该 slot
            freeSlots.remove(taskManagerSlot.getSlotId());
        });

        return optionalMatchingSlot;
    }

    // ---------------------------------------------------------------------------------------------
    // Internal slot operations
    // ---------------------------------------------------------------------------------------------

    /**
     * Registers a slot for the given task manager at the slot manager. The slot is identified by
     * the given slot id. The given resource profile defines the available resources for the slot.
     * The task manager connection can be used to communicate with the task manager.
     *
     * @param slotId                identifying the slot on the task manager
     * @param allocationId          which is currently deployed in the slot
     * @param resourceProfile       of the slot
     * @param taskManagerConnection to communicate with the remote task manager
     */
    private void registerSlot(SlotID slotId, AllocationID allocationId, JobID jobId, ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection) {
        // TODO_MA 注释： 该方法：注册一个slot

        if(slots.containsKey(slotId)) {
            // remove the old slot first
            removeSlot(slotId, new SlotManagerException(
                    String.format("Re-registration of slot %s. This indicates that the TaskExecutor has re-connected.",
                            slotId)));
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建一个 TaskManagerSlot 对象，并加入 slots 中
         */
        final TaskManagerSlot slot = createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);

        final PendingTaskManagerSlot pendingTaskManagerSlot;
        if(allocationId == null) {
            // TODO_MA 注释： 这个 slot 还没有被分配，则找到和当前 slot 的计算资源相匹配的 PendingTaskManagerSlot
            pendingTaskManagerSlot = findExactlyMatchingPendingTaskManagerSlot(resourceProfile);
        } else {
            // TODO_MA 注释： 这个 slot 已经被分配了
            pendingTaskManagerSlot = null;
        }

        // TODO_MA 注释： 两种可能： 1、slot已经被分配了
        // TODO_MA 注释： 2、没有匹配的 PendingTaskManagerSlot
        if(pendingTaskManagerSlot == null) {
            updateSlot(slotId, allocationId, jobId);
        } else {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 新注册的 slot 能够满足 PendingTaskManagerSlot 的要求
             */
            pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
            final PendingSlotRequest assignedPendingSlotRequest = pendingTaskManagerSlot
                    .getAssignedPendingSlotRequest();

            // TODO_MA 注释： PendingTaskManagerSlot 可能有关联的 PedningSlotRequest
            if(assignedPendingSlotRequest == null) {
                // TODO_MA 注释： 没有关联的 PedningSlotRequest，则将 slot 是 Free 状态
                handleFreeSlot(slot);
            } else {
                // TODO_MA 注释： 有关联的 PedningSlotRequest，则这个 request 可以被满足，分配 slot
                assignedPendingSlotRequest.unassignPendingTaskManagerSlot();
                allocateSlot(slot, assignedPendingSlotRequest);
            }
        }
    }

    @Nonnull
    private TaskManagerSlot createAndRegisterTaskManagerSlot(SlotID slotId, ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection) {
        final TaskManagerSlot slot = new TaskManagerSlot(slotId, resourceProfile, taskManagerConnection);
        slots.put(slotId, slot);
        return slot;
    }

    @Nullable
    private PendingTaskManagerSlot findExactlyMatchingPendingTaskManagerSlot(ResourceProfile resourceProfile) {
        for(PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
            if(isPendingSlotExactlyMatchingResourceProfile(pendingTaskManagerSlot, resourceProfile)) {
                return pendingTaskManagerSlot;
            }
        }

        return null;
    }

    private boolean isPendingSlotExactlyMatchingResourceProfile(PendingTaskManagerSlot pendingTaskManagerSlot,
            ResourceProfile resourceProfile) {
        return pendingTaskManagerSlot.getResourceProfile().equals(resourceProfile);
    }

    private boolean isMaxSlotNumExceededAfterRegistration(SlotReport initialSlotReport) {
        // check if the total number exceed before matching pending slot.
        if(!isMaxSlotNumExceededAfterAdding(initialSlotReport.getNumSlotStatus())) {
            return false;
        }

        // check if the total number exceed slots after consuming pending slot.
        return isMaxSlotNumExceededAfterAdding(getNumNonPendingReportedNewSlots(initialSlotReport));
    }

    private int getNumNonPendingReportedNewSlots(SlotReport slotReport) {
        final Set<TaskManagerSlotId> matchingPendingSlots = new HashSet<>();

        for(SlotStatus slotStatus : slotReport) {
            for(PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
                if(!matchingPendingSlots.contains(
                        pendingTaskManagerSlot.getTaskManagerSlotId()) && isPendingSlotExactlyMatchingResourceProfile(
                        pendingTaskManagerSlot, slotStatus.getResourceProfile())) {
                    matchingPendingSlots.add(pendingTaskManagerSlot.getTaskManagerSlotId());
                    break; // pendingTaskManagerSlot loop
                }
            }
        }
        return slotReport.getNumSlotStatus() - matchingPendingSlots.size();
    }

    /**
     * Updates a slot with the given allocation id.
     *
     * @param slotId       to update
     * @param allocationId specifying the current allocation of the slot
     * @param jobId        specifying the job to which the slot is allocated
     * @return True if the slot could be updated; otherwise false
     */
    private boolean updateSlot(SlotID slotId, AllocationID allocationId, JobID jobId) {

        // TODO_MA 注释： 根据 SlotID 获取 TaskManagerSlot 对象
        final TaskManagerSlot slot = slots.get(slotId);

        if(slot != null) {

            // TODO_MA 注释： 获取该 Slot 丛书的 TaskManager 的注册对象
            final TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

            // TODO_MA 注释： 必须注册过了，才能更改该注册成功的 TaskExecutor 之上的 Slot 的状态
            if(taskManagerRegistration != null) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 修改状态
                 */
                updateSlotState(slot, taskManagerRegistration, allocationId, jobId);

                return true;
            } else {
                throw new IllegalStateException("Trying to update a slot from a TaskManager " + slot
                        .getInstanceId() + " which has not been registered.");
            }
        } else {
            LOG.debug("Trying to update unknown slot with slot id {}.", slotId);

            return false;
        }
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *  1、每个申请 slot 的请求，都被封装成了 PendingSlotRequest
     *  2、每个 Slot 有三种基本的状态： allocated, free,  pending
     *  JobMaster 向 ResourceManager 去申请 Slot
     *  ResourceManager 经过计算之后，就把某个 TaskExecutor 之上的某个 Free 状态的 Slot assign 给当前这个 JobMaster
     *  ResourceManager 会给 JobMaster一个反馈，说我已经把某个 TaskExecutor 之上的某个 Slot 分配给你了 -- 逻辑分配
     *  此时，这个 slot 的状态，在 resourcemanager 的信息管理中个，就由 free 变成 pending
     *  ResourceManager 会发送 RPC 请求告知 TaskExecutor： 我已经把你身上的某个 Slot 分配给了某个 JobMaster
     *  此时，这个从节点，首先完成分配，然后再去联系 JobMaster，进行 slot 注册！
     *  到了这个地步，PendingSlotRequest ==> CompletedSlotRequest
     */
    private void updateSlotState(TaskManagerSlot slot, TaskManagerRegistration taskManagerRegistration,
            @Nullable AllocationID allocationId, @Nullable JobID jobId) {

        // TODO_MA 注释： 如果 allocationId 不等于空，则表示已经分配过了
        if(null != allocationId) {

            // TODO_MA 注释： 根据 Slot 的状态，进行相应的处理
            switch(slot.getState()) {

                // TODO_MA 注释： 如果是 PENDING 状态
                case PENDING:
                    // we have a pending slot request --> check whether we have to reject it
                    PendingSlotRequest pendingSlotRequest = slot.getAssignedSlotRequest();

                    if(Objects.equals(pendingSlotRequest.getAllocationId(), allocationId)) {

                        // TODO_MA 注释： 则意味着分配过了，所以取消待定的申请这个slot的请求
                        // we can cancel the slot request because it has been fulfilled
                        cancelPendingSlotRequest(pendingSlotRequest);

                        // TODO_MA 注释： 讲待定请求，移出集合
                        // remove the pending slot request, since it has been completed
                        pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());

                        // TODO_MA 注释： 完成 Slot Allocation
                        // TODO_MA 注释： 修改 Slot 的状态
                        slot.completeAllocation(allocationId, jobId);

                    } else {

                        // TODO_MA 注释： 如果不相等，则意味着，这个 Slot 已经被分配给其他 Job 的 Allocation 了
                        // we first have to free the slot in order to set a new allocationId
                        slot.clearPendingSlotRequest();
                        // set the allocation id such that the slot won't be considered for the pending slot request
                        slot.updateAllocation(allocationId, jobId);

                        // TODO_MA 注释： 取消这个 PendingSlotRequest
                        // remove the pending request if any as it has been assigned
                        final PendingSlotRequest actualPendingSlotRequest = pendingSlotRequests.remove(allocationId);
                        if(actualPendingSlotRequest != null) {
                            cancelPendingSlotRequest(actualPendingSlotRequest);
                        }

                        // TODO_MA 注释： 拒绝
                        // this will try to find a new slot for the request
                        rejectPendingSlotRequest(pendingSlotRequest, new Exception(
                                "Task manager reported slot " + slot.getSlotId() + " being already allocated."));
                    }

                    taskManagerRegistration.occupySlot();
                    break;

                // TODO_MA 注释： 是 ALLOCATED 状态
                case ALLOCATED:

                    // TODO_MA 注释： 如果这个 slot 是 ALLOCATED，但是汇报的 allocationId 和 记录的 allocationId 不一致，则释放该 Slot
                    if(!Objects.equals(allocationId, slot.getAllocationId())) {
                        slot.freeSlot();
                        slot.updateAllocation(allocationId, jobId);
                    }
                    break;

                // TODO_MA 注释： 是 FREE 状态
                case FREE:
                    // the slot is currently free --> it is stored in freeSlots
                    freeSlots.remove(slot.getSlotId());
                    slot.updateAllocation(allocationId, jobId);
                    taskManagerRegistration.occupySlot();
                    break;
            }

            fulfilledSlotRequests.put(allocationId, slot.getSlotId());
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 否则未分配过
         */
        else {
            // no allocation reported
            switch(slot.getState()) {
                case FREE:
                    handleFreeSlot(slot);
                    break;
                case PENDING:
                    // don't do anything because we still have a pending slot request
                    break;
                case ALLOCATED:
                    AllocationID oldAllocation = slot.getAllocationId();
                    slot.freeSlot();
                    fulfilledSlotRequests.remove(oldAllocation);
                    taskManagerRegistration.freeSlot();

                    handleFreeSlot(slot);
                    break;
            }
        }
    }

    /**
     * Tries to allocate a slot for the given slot request. If there is no slot available, the
     * resource manager is informed to allocate more resources and a timeout for the request is
     * registered.
     *
     * @param pendingSlotRequest to allocate a slot for
     * @throws ResourceManagerException if the slot request failed or is unfulfillable
     */
    private void internalRequestSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {

        // TODO_MA 注释： 先从请求对象中，拿到需要申请的 slot 的资源配置
        final ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();

        // TODO_MA 注释： 首先从 FREE 状态的已注册的 slot 中选择符合要求的 slot
        OptionalConsumer.of(findMatchingSlot(resourceProfile))

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 找到了符合条件的slot，完成逻辑分配
                 *  -
                 *  关于参数：
                 *  1、taskManagerSlot 是 当前符合要求的 slot 的一个资源抽象对象
                 *  2、pendingSlotRequest 对应的请求对象
                 *  其实这就是一种逻辑分派！
                 *  -
                 *  内部的细节动作：
                 *  1、完成逻辑分配： 内部会有关于 slot 的状态修改
                 *      findMatchingSlot(resourceProfile)
                 *      完成映射： taskManagerSlot ==> pendingSlotRequest
                 *  2、发送RPC 请求给对应的从节点
                 */
                .ifPresent(taskManagerSlot -> allocateSlot(taskManagerSlot, pendingSlotRequest))

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 等！
                 *  两个方面：
                 *  1、如果是 flink  standalone : 等其他的slot释放！
                 *  2、如果是 flink on yarn 模式，请注意： 我们可以多启动几个 TaskManager
                 *  真实情况，就是主节点 Flink 主节点 发送请求给 YARN RM，申请 container ，在 container 中启动 TaksManager
                 */
                .ifNotPresent(() -> fulfillPendingSlotRequestWithPendingTaskManagerSlot(pendingSlotRequest)
        );
    }

    private void fulfillPendingSlotRequestWithPendingTaskManagerSlot(
            PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
        ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();

        // TODO_MA 注释： 从 PendingTaskManagerSlot 中选择
        Optional<PendingTaskManagerSlot> pendingTaskManagerSlotOptional = findFreeMatchingPendingTaskManagerSlot(
                resourceProfile);

        // TODO_MA 注释： 如果连 PendingTaskManagerSlot 中都没有
        if(!pendingTaskManagerSlotOptional.isPresent()) {

            // TODO_MA 注释： 请求 ResourceManager 分配资源，通过 ResourceActions#allocateResource(ResourceProfile) 回调进行
            pendingTaskManagerSlotOptional = allocateResource(resourceProfile);
        }

        // TODO_MA 注释： 将 PendingTaskManagerSlot 指派给 PendingSlotRequest
        OptionalConsumer.of(pendingTaskManagerSlotOptional)

                // TODO_MA 注释： 如果存在
                .ifPresent(pendingTaskManagerSlot -> assignPendingTaskManagerSlot(pendingSlotRequest,
                        pendingTaskManagerSlot))

                // TODO_MA 注释： 如果不存在
                .ifNotPresent(() -> {
                    // request can not be fulfilled by any free slot or pending slot that
                    // can be allocated, check whether it can be fulfilled by allocated slots
                    if(failUnfulfillableRequest && !isFulfillableByRegisteredOrPendingSlots(
                            pendingSlotRequest.getResourceProfile())) {
                        throw new UnfulfillableSlotRequestException(pendingSlotRequest.getAllocationId(),
                                pendingSlotRequest.getResourceProfile());
                    }
                });
    }

    private Optional<PendingTaskManagerSlot> findFreeMatchingPendingTaskManagerSlot(
            ResourceProfile requiredResourceProfile) {
        for(PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
            if(pendingTaskManagerSlot.getAssignedPendingSlotRequest() == null && pendingTaskManagerSlot
                    .getResourceProfile().isMatching(requiredResourceProfile)) {
                return Optional.of(pendingTaskManagerSlot);
            }
        }

        return Optional.empty();
    }

    private boolean isFulfillableByRegisteredOrPendingSlots(ResourceProfile resourceProfile) {
        for(TaskManagerSlot slot : slots.values()) {
            if(slot.getResourceProfile().isMatching(resourceProfile)) {
                return true;
            }
        }
        for(PendingTaskManagerSlot slot : pendingSlots.values()) {
            if(slot.getResourceProfile().isMatching(resourceProfile)) {
                return true;
            }
        }
        return false;
    }

    private boolean isMaxSlotNumExceededAfterAdding(int numNewSlot) {
        return getNumberRegisteredSlots() + getNumberPendingTaskManagerSlots() + numNewSlot > maxSlotNum;
    }

    private void allocateRedundantTaskManagers(int number) {
        int allocatedNumber = allocateResources(number);
        if(number != allocatedNumber) {
            LOG.warn("Expect to allocate {} taskManagers. Actually allocate {} taskManagers.", number, allocatedNumber);
        }
    }

    /**
     * Allocate a number of workers based on the input param.
     *
     * @param workerNum the number of workers to allocate.
     * @return the number of allocated workers successfully.
     */
    private int allocateResources(int workerNum) {
        int allocatedWorkerNum = 0;
        for(int i = 0; i < workerNum; ++i) {
            if(allocateResource(defaultSlotResourceProfile).isPresent()) {
                ++allocatedWorkerNum;
            } else {
                break;
            }
        }
        return allocatedWorkerNum;
    }

    private Optional<PendingTaskManagerSlot> allocateResource(ResourceProfile requestedSlotResourceProfile) {
        final int numRegisteredSlots = getNumberRegisteredSlots();
        final int numPendingSlots = getNumberPendingTaskManagerSlots();
        if(isMaxSlotNumExceededAfterAdding(numSlotsPerWorker)) {
            LOG.warn(
                    "Could not allocate {} more slots. The number of registered and pending slots is {}, while the maximum is {}.",
                    numSlotsPerWorker, numPendingSlots + numRegisteredSlots, maxSlotNum);
            return Optional.empty();
        }

        if(!defaultSlotResourceProfile.isMatching(requestedSlotResourceProfile)) {
            // requested resource profile is unfulfillable
            return Optional.empty();
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 如果是 YARN 模式，则在此 申请 ResourceManager 启动 TaskManager 提供更多可用的 slot
         */
        if(!resourceActions.allocateResource(defaultWorkerResourceSpec)) {
            // resource cannot be allocated
            return Optional.empty();
        }

        PendingTaskManagerSlot pendingTaskManagerSlot = null;
        for(int i = 0; i < numSlotsPerWorker; ++i) {
            pendingTaskManagerSlot = new PendingTaskManagerSlot(defaultSlotResourceProfile);
            pendingSlots.put(pendingTaskManagerSlot.getTaskManagerSlotId(), pendingTaskManagerSlot);
        }

        return Optional
                .of(Preconditions.checkNotNull(pendingTaskManagerSlot, "At least one pending slot should be created."));
    }

    private void assignPendingTaskManagerSlot(PendingSlotRequest pendingSlotRequest,
            PendingTaskManagerSlot pendingTaskManagerSlot) {
        pendingTaskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
        pendingSlotRequest.assignPendingTaskManagerSlot(pendingTaskManagerSlot);
    }

    /**
     * Allocates the given slot for the given slot request. This entails sending a registration
     * message to the task manager and treating failures.
     *
     * @param taskManagerSlot    to allocate for the given slot request
     * @param pendingSlotRequest to allocate the given slot for
     */
    private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
        Preconditions.checkState(taskManagerSlot.getState() == SlotState.FREE);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
        TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

        final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
        final AllocationID allocationId = pendingSlotRequest.getAllocationId();
        final SlotID slotId = taskManagerSlot.getSlotId();
        final InstanceID instanceID = taskManagerSlot.getInstanceId();

        // TODO_MA 注释： 执行到这儿，意味着 ResourceManager 已经完成了逻辑分配
        // TODO_MA 注释： taskManagerSlot 状态变为 PENDING
        taskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
        pendingSlotRequest.setRequestFuture(completableFuture);

        // TODO_MA 注释： 如果有 PendingTaskManager 指派给当前 pendingSlotRequest，要先解除关联
        returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);

        if(taskManagerRegistration == null) {
            throw new IllegalStateException(
                    "Could not find a registered task manager for instance id " + instanceID + '.');
        }

        taskManagerRegistration.markUsed();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： ResourceManager 的 SlotManager 将 分派的 Slot 对应的 SlotRequest 请求发送给该 TaskExecutor
         */
        // RPC call to the task manager
        CompletableFuture<Acknowledge> requestFuture = gateway
                .requestSlot(slotId, pendingSlotRequest.getJobId(), allocationId,
                        pendingSlotRequest.getResourceProfile(), pendingSlotRequest.getTargetAddress(),
                        resourceManagerId, taskManagerRequestTimeout);

        // TODO_MA 注释： RPC调用的请求完成
        requestFuture.whenComplete((Acknowledge acknowledge, Throwable throwable) -> {
            if(acknowledge != null) {
                completableFuture.complete(acknowledge);
            } else {
                completableFuture.completeExceptionally(throwable);
            }
        });

        // TODO_MA 注释： PendingSlotRequest 请求完成可能是由于上面 RPC 调用完成，也可能是因为 PendingSlotRequest 被取消
        completableFuture.whenCompleteAsync((Acknowledge acknowledge, Throwable throwable) -> {
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 如果 RPC 请求完成，并且每报错，则进行状态修改处理
                 */
                if(acknowledge != null) {

                    // TODO_MA 注释： 如果请求成功，则取消 pendingSlotRequest，并更新 slot 状态 PENDING -> ALLOCATED
                    updateSlot(slotId, allocationId, pendingSlotRequest.getJobId());
                } else {
                    if(throwable instanceof SlotOccupiedException) {
                        // TODO_MA 注释： 这个 slot 已经被占用了，更新状态
                        SlotOccupiedException exception = (SlotOccupiedException) throwable;
                        updateSlot(slotId, exception.getAllocationId(), exception.getJobId());
                    } else {
                        // TODO_MA 注释： 请求失败，将 pendingSlotRequest 从 TaskManagerSlot 中移除
                        removeSlotRequestFromSlot(slotId, allocationId);
                    }

                    if(!(throwable instanceof CancellationException)) {
                        // TODO_MA 注释： slot request 请求失败，会进行重试
                        handleFailedSlotRequest(slotId, allocationId, throwable);
                    } else {
                        // TODO_MA 注释： 主动取消
                        LOG.debug("Slot allocation request {} has been cancelled.", allocationId, throwable);
                    }
                }
            } catch(Exception e) {
                LOG.error("Error while completing the slot allocation.", e);
            }
        }, mainThreadExecutor);
    }

    private void returnPendingTaskManagerSlotIfAssigned(PendingSlotRequest pendingSlotRequest) {
        final PendingTaskManagerSlot pendingTaskManagerSlot = pendingSlotRequest.getAssignedPendingTaskManagerSlot();
        if(pendingTaskManagerSlot != null) {
            pendingTaskManagerSlot.unassignPendingSlotRequest();
            pendingSlotRequest.unassignPendingTaskManagerSlot();
        }
    }

    /**
     * Handles a free slot. It first tries to find a pending slot request which can be fulfilled. If
     * there is no such request, then it will add the slot to the set of free slots.
     *
     * @param freeSlot to find a new slot request for
     */
    private void handleFreeSlot(TaskManagerSlot freeSlot) {
        Preconditions.checkState(freeSlot.getState() == SlotState.FREE);

        // TODO_MA 注释： 先查找是否有能够满足的 PendingSlotRequest
        PendingSlotRequest pendingSlotRequest = findMatchingRequest(freeSlot.getResourceProfile());

        if(null != pendingSlotRequest) {

            // TODO_MA 注释： 如果有匹配的 PendingSlotRequest，则分配slot
            allocateSlot(freeSlot, pendingSlotRequest);
        } else {
            freeSlots.put(freeSlot.getSlotId(), freeSlot);
        }
    }

    /**
     * Removes the given set of slots from the slot manager.
     *
     * @param slotsToRemove identifying the slots to remove from the slot manager
     * @param cause         for removing the slots
     */
    private void removeSlots(Iterable<SlotID> slotsToRemove, Exception cause) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 移除注销的 TaskManager 上的所有的 Slot
         */
        for(SlotID slotId : slotsToRemove) {
            removeSlot(slotId, cause);
        }
    }

    /**
     * Removes the given slot from the slot manager.
     *
     * @param slotId identifying the slot to remove
     * @param cause  for removing the slot
     */
    private void removeSlot(SlotID slotId, Exception cause) {

        // TODO_MA 注释： 移除
        TaskManagerSlot slot = slots.remove(slotId);

        if(null != slot) {

            // TODO_MA 注释： 移除
            freeSlots.remove(slotId);

            // TODO_MA 注释： 如果有 PendingSlotRequest 则直接拒绝
            if(slot.getState() == SlotState.PENDING) {
                // reject the pending slot request --> triggering a new allocation attempt
                rejectPendingSlotRequest(slot.getAssignedSlotRequest(), cause);
            }

            AllocationID oldAllocationId = slot.getAllocationId();

            if(oldAllocationId != null) {

                // TODO_MA 注释： 移除
                fulfilledSlotRequests.remove(oldAllocationId);

                // TODO_MA 注释： 通知失败
                resourceActions.notifyAllocationFailure(slot.getJobId(), oldAllocationId, cause);
            }
        } else {
            LOG.debug("There was no slot registered with slot id {}.", slotId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Internal request handling methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Removes a pending slot request identified by the given allocation id from a slot identified
     * by the given slot id.
     *
     * @param slotId       identifying the slot
     * @param allocationId identifying the presumable assigned pending slot request
     */
    private void removeSlotRequestFromSlot(SlotID slotId, AllocationID allocationId) {
        TaskManagerSlot taskManagerSlot = slots.get(slotId);

        if(null != taskManagerSlot) {
            if(taskManagerSlot.getState() == SlotState.PENDING && Objects
                    .equals(allocationId, taskManagerSlot.getAssignedSlotRequest().getAllocationId())) {

                TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations
                        .get(taskManagerSlot.getInstanceId());

                if(taskManagerRegistration == null) {
                    throw new IllegalStateException(
                            "Trying to remove slot request from slot for which there is no TaskManager " + taskManagerSlot
                                    .getInstanceId() + " is registered.");
                }

                // clear the pending slot request
                taskManagerSlot.clearPendingSlotRequest();

                updateSlotState(taskManagerSlot, taskManagerRegistration, null, null);
            } else {
                LOG.debug("Ignore slot request removal for slot {}.", slotId);
            }
        } else {
            LOG.debug("There was no slot with {} registered. Probably this slot has been already freed.", slotId);
        }
    }

    /**
     * Handles a failed slot request. The slot manager tries to find a new slot fulfilling the
     * resource requirements for the failed slot request.
     *
     * @param slotId       identifying the slot which was assigned to the slot request before
     * @param allocationId identifying the failed slot request
     * @param cause        of the failure
     */
    private void handleFailedSlotRequest(SlotID slotId, AllocationID allocationId, Throwable cause) {
        PendingSlotRequest pendingSlotRequest = pendingSlotRequests.get(allocationId);

        LOG.debug("Slot request with allocation id {} failed for slot {}.", allocationId, slotId, cause);

        if(null != pendingSlotRequest) {
            pendingSlotRequest.setRequestFuture(null);

            try {
                internalRequestSlot(pendingSlotRequest);
            } catch(ResourceManagerException e) {
                pendingSlotRequests.remove(allocationId);

                resourceActions.notifyAllocationFailure(pendingSlotRequest.getJobId(), allocationId, e);
            }
        } else {
            LOG.debug(
                    "There was not pending slot request with allocation id {}. Probably the request has been fulfilled or cancelled.",
                    allocationId);
        }
    }

    /**
     * Rejects the pending slot request by failing the request future with a {@link
     * SlotAllocationException}.
     *
     * @param pendingSlotRequest to reject
     * @param cause              of the rejection
     */
    private void rejectPendingSlotRequest(PendingSlotRequest pendingSlotRequest, Exception cause) {
        CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

        if(null != request) {
            request.completeExceptionally(new SlotAllocationException(cause));
        } else {
            LOG.debug("Cannot reject pending slot request {}, since no request has been sent.",
                    pendingSlotRequest.getAllocationId());
        }
    }

    /**
     * Cancels the given slot request.
     *
     * @param pendingSlotRequest to cancel
     */
    private void cancelPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
        CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

        // TODO_MA 注释：
        returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

        if(null != request) {
            request.cancel(false);
        }
    }

    @VisibleForTesting
    public static ResourceProfile generateDefaultSlotResourceProfile(WorkerResourceSpec workerResourceSpec,
            int numSlotsPerWorker) {
        return ResourceProfile.newBuilder().setCpuCores(workerResourceSpec.getCpuCores().divide(numSlotsPerWorker))
                .setTaskHeapMemory(workerResourceSpec.getTaskHeapSize().divide(numSlotsPerWorker))
                .setTaskOffHeapMemory(workerResourceSpec.getTaskOffHeapSize().divide(numSlotsPerWorker))
                .setManagedMemory(workerResourceSpec.getManagedMemSize().divide(numSlotsPerWorker))
                .setNetworkMemory(workerResourceSpec.getNetworkMemSize().divide(numSlotsPerWorker)).build();
    }

    // ---------------------------------------------------------------------------------------------
    // Internal periodic check methods
    // ---------------------------------------------------------------------------------------------

    @VisibleForTesting
    void checkTaskManagerTimeoutsAndRedundancy() {
        if(!taskManagerRegistrations.isEmpty()) {
            long currentTime = System.currentTimeMillis();

            ArrayList<TaskManagerRegistration> timedOutTaskManagers = new ArrayList<>(taskManagerRegistrations.size());

            // first retrieve the timed out TaskManagers
            for(TaskManagerRegistration taskManagerRegistration : taskManagerRegistrations.values()) {
                if(currentTime - taskManagerRegistration.getIdleSince() >= taskManagerTimeout.toMilliseconds()) {
                    // we collect the instance ids first in order to avoid concurrent modifications
                    // by the
                    // ResourceActions.releaseResource call
                    timedOutTaskManagers.add(taskManagerRegistration);
                }
            }

            int slotsDiff = redundantTaskManagerNum * numSlotsPerWorker - freeSlots.size();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            if(freeSlots.size() == slots.size()) {
                // No need to keep redundant taskManagers if no job is running.
                releaseTaskExecutors(timedOutTaskManagers, timedOutTaskManagers.size());
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            else if(slotsDiff > 0) {
                // Keep enough redundant taskManagers from time to time.
                int requiredTaskManagers = MathUtils.divideRoundUp(slotsDiff, numSlotsPerWorker);
                allocateRedundantTaskManagers(requiredTaskManagers);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            else {
                // second we trigger the release resource callback which can decide upon the resource release
                int maxReleaseNum = (-slotsDiff) / numSlotsPerWorker;
                releaseTaskExecutors(timedOutTaskManagers, Math.min(maxReleaseNum, timedOutTaskManagers.size()));
            }
        }
    }

    private void releaseTaskExecutors(ArrayList<TaskManagerRegistration> timedOutTaskManagers, int releaseNum) {
        for(int index = 0; index < releaseNum; ++index) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            if(waitResultConsumedBeforeRelease) {
                releaseTaskExecutorIfPossible(timedOutTaskManagers.get(index));
            } else {
                releaseTaskExecutor(timedOutTaskManagers.get(index).getInstanceId());
            }
        }
    }

    private void releaseTaskExecutorIfPossible(TaskManagerRegistration taskManagerRegistration) {
        long idleSince = taskManagerRegistration.getIdleSince();
        taskManagerRegistration.getTaskManagerConnection().getTaskExecutorGateway().canBeReleased()
                .thenAcceptAsync(canBeReleased -> {
                    InstanceID timedOutTaskManagerId = taskManagerRegistration.getInstanceId();
                    boolean stillIdle = idleSince == taskManagerRegistration.getIdleSince();
                    if(stillIdle && canBeReleased) {

                        /*************************************************
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释：
                         */
                        releaseTaskExecutor(timedOutTaskManagerId);
                    }
                }, mainThreadExecutor);
    }

    private void releaseTaskExecutor(InstanceID timedOutTaskManagerId) {
        final FlinkException cause = new FlinkException("TaskExecutor exceeded the idle timeout.");
        LOG.debug("Release TaskExecutor {} because it exceeded the idle timeout.", timedOutTaskManagerId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        resourceActions.releaseResource(timedOutTaskManagerId, cause);
    }

    private void checkSlotRequestTimeouts() {
        if(!pendingSlotRequests.isEmpty()) {
            long currentTime = System.currentTimeMillis();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 获取所有的 处于待定状态中的 SlotRequest
             */
            Iterator<Map.Entry<AllocationID, PendingSlotRequest>> slotRequestIterator = pendingSlotRequests.entrySet()
                    .iterator();

            while(slotRequestIterator.hasNext()) {
                PendingSlotRequest slotRequest = slotRequestIterator.next().getValue();

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 执行超时校验
                 */
                if(currentTime - slotRequest.getCreationTimestamp() >= slotRequestTimeout.toMilliseconds()) {
                    slotRequestIterator.remove();

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： Assigned
                     *  ResourceManager 已经分配某个 TaskManager 上的某个 Slot 给了某个 Job
                     *  但是，到现在为止： TaskManager 还不知道！
                     */
                    if(slotRequest.isAssigned()) {
                        cancelPendingSlotRequest(slotRequest);
                    }

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 通知失败！
                     */
                    resourceActions.notifyAllocationFailure(slotRequest.getJobId(), slotRequest.getAllocationId(),
                            new TimeoutException("The allocation could not be fulfilled in time."));
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void internalUnregisterTaskManager(TaskManagerRegistration taskManagerRegistration, Exception cause) {
        Preconditions.checkNotNull(taskManagerRegistration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        removeSlots(taskManagerRegistration.getSlots(), cause);
    }

    private boolean checkDuplicateRequest(AllocationID allocationId) {
        return pendingSlotRequests.containsKey(allocationId) || fulfilledSlotRequests.containsKey(allocationId);
    }

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
    }

    // ---------------------------------------------------------------------------------------------
    // Testing methods
    // ---------------------------------------------------------------------------------------------

    @VisibleForTesting
    TaskManagerSlot getSlot(SlotID slotId) {
        return slots.get(slotId);
    }

    @VisibleForTesting
    PendingSlotRequest getSlotRequest(AllocationID allocationId) {
        return pendingSlotRequests.get(allocationId);
    }

    @VisibleForTesting
    boolean isTaskManagerIdle(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if(null != taskManagerRegistration) {
            return taskManagerRegistration.isIdle();
        } else {
            return false;
        }
    }
}
