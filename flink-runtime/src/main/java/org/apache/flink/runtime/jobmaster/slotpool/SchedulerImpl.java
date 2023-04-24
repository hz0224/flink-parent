/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Scheduler that assigns tasks to slots. This class is currently work in progress, comments will be
 * updated as we move forward.
 */
public class SchedulerImpl implements Scheduler {

    private static final Logger log = LoggerFactory.getLogger(SchedulerImpl.class);

    private static final int DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE = 128;

    /**
     * Strategy that selects the best slot for a given slot allocation request.
     * // TODO_MA 注释： 用于从一组 slot 中选出最符合资源申请偏好的一个
     */
    @Nonnull
    private final SlotSelectionStrategy slotSelectionStrategy;

    /**
     * The slot pool from which slots are allocated.
     * // TODO_MA 注释： 借助 SlotPool 来申请 PhysicalSlot
     */
    @Nonnull
    private final SlotPool slotPool;

    /**
     * Executor for running tasks in the job master's main thread.
     */
    @Nonnull
    private ComponentMainThreadExecutor componentMainThreadExecutor;

    /**
     * Managers for the different slot sharing groups.
     * // TODO_MA 注释： 借助 SlotSharingManager 实现 slot 共享
     */
    @Nonnull
    private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers;

    public SchedulerImpl(@Nonnull SlotSelectionStrategy slotSelectionStrategy, @Nonnull SlotPool slotPool) {
        this(slotSelectionStrategy, slotPool, new HashMap<>(DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE));
    }

    @VisibleForTesting
    public SchedulerImpl(@Nonnull SlotSelectionStrategy slotSelectionStrategy, @Nonnull SlotPool slotPool,
            @Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers) {

        this.slotSelectionStrategy = slotSelectionStrategy;
        this.slotSharingManagers = slotSharingManagers;
        this.slotPool = slotPool;
        this.componentMainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
                "Scheduler is not initialized with proper main thread executor. " + "Call to Scheduler.start(...) required.");
    }

    @Override
    public void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor) {
        this.componentMainThreadExecutor = mainThreadExecutor;
    }

    // ---------------------------

    @Override
    public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile,
            Time allocationTimeout) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return allocateSlotInternal(slotRequestId, scheduledUnit, slotProfile, allocationTimeout);
    }

    @Override
    public CompletableFuture<LogicalSlot> allocateBatchSlot(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile) {
        return allocateSlotInternal(slotRequestId, scheduledUnit, slotProfile, null);
    }

    @Nonnull
    private CompletableFuture<LogicalSlot> allocateSlotInternal(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile,
            @Nullable Time allocationTimeout) {
        log.debug("Received slot request [{}] for task: {}", slotRequestId, scheduledUnit.getJobVertexId());

        componentMainThreadExecutor.assertRunningInMainThread();

        final CompletableFuture<LogicalSlot> allocationResultFuture = new CompletableFuture<>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        internalAllocateSlot(allocationResultFuture, slotRequestId, scheduledUnit, slotProfile, allocationTimeout);
        return allocationResultFuture;
    }

    private void internalAllocateSlot(CompletableFuture<LogicalSlot> allocationResultFuture, SlotRequestId slotRequestId, ScheduledUnit scheduledUnit,
            SlotProfile slotProfile, Time allocationTimeout) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        CompletableFuture<LogicalSlot> allocationFuture = allocateSharedSlot(slotRequestId, scheduledUnit, slotProfile, allocationTimeout);

        allocationFuture.whenComplete((LogicalSlot slot, Throwable failure) -> {
            if(failure != null) {
                cancelSlotRequest(slotRequestId, scheduledUnit.getSlotSharingGroupId(), failure);
                allocationResultFuture.completeExceptionally(failure);
            } else {
                allocationResultFuture.complete(slot);
            }
        });
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {

        componentMainThreadExecutor.assertRunningInMainThread();

        if(slotSharingGroupId != null) {
            releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
        } else {
            slotPool.releaseSlot(slotRequestId, cause);
        }
    }

    @Override
    public void returnLogicalSlot(LogicalSlot logicalSlot) {
        SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
        SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
        FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");
        cancelSlotRequest(slotRequestId, slotSharingGroupId, cause);
    }

    // ---------------------------

    @Nonnull
    private CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(SlotRequestId slotRequestId, SlotProfile slotProfile, @Nullable Time allocationTimeout) {
        if(allocationTimeout == null) {
            return slotPool.requestNewAllocatedBatchSlot(slotRequestId, slotProfile.getPhysicalSlotResourceProfile());
        } else {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： Scheduler 转交到 SlotPool 去申请一个 Slot 资源
             */
            return slotPool.requestNewAllocatedSlot(slotRequestId, slotProfile.getPhysicalSlotResourceProfile(), allocationTimeout);
        }
    }

    private Optional<SlotAndLocality> tryAllocateFromAvailable(@Nonnull SlotRequestId slotRequestId, @Nonnull SlotProfile slotProfile) {

        Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList = slotPool.getAvailableSlotsInformation().stream()
                .map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot).collect(Collectors.toList());

        Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot = slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

        return selectedAvailableSlot.flatMap(slotInfoAndLocality -> {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            Optional<PhysicalSlot> optionalAllocatedSlot = slotPool.allocateAvailableSlot(slotRequestId, slotInfoAndLocality.getSlotInfo().getAllocationId());

            return optionalAllocatedSlot.map(allocatedSlot -> new SlotAndLocality(allocatedSlot, slotInfoAndLocality.getLocality()));
        });
    }

    // ------------------------------- slot sharing code

    private CompletableFuture<LogicalSlot> allocateSharedSlot(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile,
            @Nullable Time allocationTimeout) {

        // TODO_MA 注释： 每一个 SlotSharingGroup 对应一个 SlotSharingManager
        // allocate slot with slot sharing
        final SlotSharingManager multiTaskSlotManager = slotSharingManagers
                .computeIfAbsent(scheduledUnit.getSlotSharingGroupId(), id -> new SlotSharingManager(id, slotPool, this));

        // TODO_MA 注释： 分配 MultiTaskSlot
        final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality;
        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： CoLocation
             *  上游 Task 的分区编号  和 下游 Task 的分区编号，必须一一对应
             *  那么这样的话， 那么上游 Task 和 下游 Task 对应编号的 Task 就可以通过 slot 共享来节省资源
             *  －
             *  Slot共享：  同一个 job 中的不同 Operator 类型的 SubTask 可以运行在一个 SLOT 之上
             */
            if(scheduledUnit.getCoLocationConstraint() != null) {

                // TODO_MA 注释： 存在 ColLocation 约束
                multiTaskSlotLocality = allocateCoLocatedMultiTaskSlot(scheduledUnit.getCoLocationConstraint(), multiTaskSlotManager, slotProfile,
                        allocationTimeout);
            } else {

                // TODO_MA 注释： 不存在， 它的内部，也是调用
                multiTaskSlotLocality = allocateMultiTaskSlot(scheduledUnit.getJobVertexId(), multiTaskSlotManager, slotProfile, allocationTimeout);
            }
        } catch(NoResourceAvailableException noResourceException) {
            return FutureUtils.completedExceptionally(noResourceException);
        }

        // sanity check
        Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(scheduledUnit.getJobVertexId()));

        // TODO_MA 注释： 在 MultiTaskSlot 下创建叶子节点 SingleTaskSlot，并获取可以分配给任务的 SingleLogicalSlot
        final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality.getMultiTaskSlot()

                // TODO_MA 注释：
                .allocateSingleTaskSlot(slotRequestId, slotProfile.getTaskResourceProfile(), scheduledUnit.getJobVertexId(),
                        multiTaskSlotLocality.getLocality());
        return leaf.getLogicalSlotFuture();
    }

    /**
     * Allocates a co-located {@link SlotSharingManager.MultiTaskSlot} for the given {@link
     * CoLocationConstraint}.
     *
     * <p>The returned {@link SlotSharingManager.MultiTaskSlot} can be uncompleted.
     *
     * @param coLocationConstraint for which to allocate a {@link SlotSharingManager.MultiTaskSlot}
     * @param multiTaskSlotManager responsible for the slot sharing group for which to allocate the
     *                             slot
     * @param slotProfile          specifying the requirements for the requested slot
     * @param allocationTimeout    timeout before the slot allocation times out
     * @return A {@link SlotAndLocality} which contains the allocated{@link
     * SlotSharingManager.MultiTaskSlot} and its locality wrt the given location preferences
     */
    private SlotSharingManager.MultiTaskSlotLocality allocateCoLocatedMultiTaskSlot(CoLocationConstraint coLocationConstraint,
            SlotSharingManager multiTaskSlotManager, SlotProfile slotProfile, @Nullable Time allocationTimeout) throws NoResourceAvailableException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  coLocationConstraint 会和分配给它的 MultiTaskSlot(不是root) 的 SlotRequestId 绑定
         *  这个绑定关系只有在分配了 MultiTaskSlot 之后才会生成
         *  可以根据 SlotRequestId 直接定位到 MultiTaskSlot
         */
        final SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();

        if(coLocationSlotRequestId != null) {
            // we have a slot assigned --> try to retrieve it
            final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);

            if(taskSlot != null) {
                Preconditions.checkState(taskSlot instanceof SlotSharingManager.MultiTaskSlot);

                SlotSharingManager.MultiTaskSlot multiTaskSlot = (SlotSharingManager.MultiTaskSlot) taskSlot;

                if(multiTaskSlot.mayHaveEnoughResourcesToFulfill(slotProfile.getTaskResourceProfile())) {
                    return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.LOCAL);
                }

                throw new NoResourceAvailableException("Not enough resources in the slot for all co-located tasks.");
            } else {
                // the slot may have been cancelled in the mean time
                coLocationConstraint.setSlotRequestId(null);
            }
        }

        if(coLocationConstraint.isAssigned()) {
            // refine the preferred locations of the slot profile
            slotProfile = SlotProfile.priorAllocation(slotProfile.getTaskResourceProfile(), slotProfile.getPhysicalSlotResourceProfile(),
                    Collections.singleton(coLocationConstraint.getLocation()), slotProfile.getPreferredAllocations(),
                    slotProfile.getPreviousExecutionGraphAllocations());
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 为这个 coLocationConstraint 分配 MultiTaskSlot，先找到符合要求的root MultiTaskSlot
         */
        // get a new multi task slot
        SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(coLocationConstraint.getGroupId(), multiTaskSlotManager,
                slotProfile, allocationTimeout);

        // check whether we fulfill the co-location constraint
        if(coLocationConstraint.isAssigned() && multiTaskSlotLocality.getLocality() != Locality.LOCAL) {
            multiTaskSlotLocality.getMultiTaskSlot()
                    .release(new FlinkException("Multi task slot is not local and, thus, does not fulfill the co-location constraint."));

            throw new NoResourceAvailableException(
                    "Could not allocate a local multi task slot for the " + "co location constraint " + coLocationConstraint + '.');
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 在 root MultiTaskSlot 下面创建一个二级的 MultiTaskSlot，分配给这个 coLocationConstraint
         */
        final SlotRequestId slotRequestId = new SlotRequestId();
        final SlotSharingManager.MultiTaskSlot coLocationSlot = multiTaskSlotLocality.getMultiTaskSlot()
                .allocateMultiTaskSlot(slotRequestId, coLocationConstraint.getGroupId());

        // TODO_MA 注释： 为 coLocationConstraint 绑定 slotRequestId，后续就可以直接通过这个 slotRequestId 定位到 MultiTaskSlot
        // mark the requested slot as co-located slot for other co-located tasks
        coLocationConstraint.setSlotRequestId(slotRequestId);

        // lock the co-location constraint once we have obtained the allocated slot
        coLocationSlot.getSlotContextFuture().whenComplete((SlotContext slotContext, Throwable throwable) -> {
            if(throwable == null) {
                // check whether we are still assigned to the co-location constraint
                if(Objects.equals(coLocationConstraint.getSlotRequestId(), slotRequestId)) {
                    // TODO_MA 注释： 为这个 coLocationConstraint 绑定位置
                    coLocationConstraint.lockLocation(slotContext.getTaskManagerLocation());
                } else {
                    log.debug("Failed to lock colocation constraint {} because assigned slot " + "request {} differs from fulfilled slot request {}.",
                            coLocationConstraint.getGroupId(), coLocationConstraint.getSlotRequestId(), slotRequestId);
                }
            } else {
                log.debug("Failed to lock colocation constraint {} because the slot " + "allocation for slot request {} failed.",
                        coLocationConstraint.getGroupId(), coLocationConstraint.getSlotRequestId(), throwable);
            }
        });

        return SlotSharingManager.MultiTaskSlotLocality.of(coLocationSlot, multiTaskSlotLocality.getLocality());
    }

    /**
     * Allocates a {@link SlotSharingManager.MultiTaskSlot} for the given groupId which is in the
     * slot sharing group for which the given {@link SlotSharingManager} is responsible.
     *
     * <p>The method can return an uncompleted {@link SlotSharingManager.MultiTaskSlot}.
     *
     * @param groupId            for which to allocate a new {@link SlotSharingManager.MultiTaskSlot}
     * @param slotSharingManager responsible for the slot sharing group for which to allocate the
     *                           slot
     * @param slotProfile        slot profile that specifies the requirements for the slot
     * @param allocationTimeout  timeout before the slot allocation times out; null if requesting a
     *                           batch slot
     * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated
     * {@link SlotSharingManager.MultiTaskSlot} and its locality wrt the given location
     * preferences
     */
    private SlotSharingManager.MultiTaskSlotLocality allocateMultiTaskSlot(AbstractID groupId, SlotSharingManager slotSharingManager, SlotProfile slotProfile,
            @Nullable Time allocationTimeout) {

        // TODO_MA 注释： 找到符合要求的已经分配了 AllocatedSlot 的 root MultiTaskSlot 集合，
        // TODO_MA 注释： 这里的符合要求是指 root MultiTaskSlot 不含有当前 groupId,
        // TODO_MA 注释： 避免把 groupId 相同（同一个 JobVertex）的不同 task 分配到同一个 slot 中
        Collection<SlotSelectionStrategy.SlotInfoAndResources> resolvedRootSlotsInfo = slotSharingManager.listResolvedRootSlotInfo(groupId);

        // TODO_MA 注释： 由 slotSelectionStrategy 选出最符合条件的
        SlotSelectionStrategy.SlotInfoAndLocality bestResolvedRootSlotWithLocality = slotSelectionStrategy
                .selectBestSlotForProfile(resolvedRootSlotsInfo, slotProfile).orElse(null);

        // TODO_MA 注释： 对 MultiTaskSlot 和 Locality 做一层封装
        final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = bestResolvedRootSlotWithLocality != null ? new SlotSharingManager.MultiTaskSlotLocality(
                slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotWithLocality.getSlotInfo()), bestResolvedRootSlotWithLocality.getLocality()) : null;

        // TODO_MA 注释： 如果 MultiTaskSlot 对应的 AllocatedSlot 和请求偏好的 slot 落在同一个 TaskManager，那么就选择这个 MultiTaskSlot
        if(multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
            return multiTaskSlotLocality;
        }

        final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
        final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 先尝试从 SlotPool 可用的 AllocatedSlot 中获取
         */
        Optional<SlotAndLocality> optionalPoolSlotAndLocality = tryAllocateFromAvailable(allocatedSlotRequestId, slotProfile);

        // TODO_MA 注释： 如果存在本地性
        if(optionalPoolSlotAndLocality.isPresent()) {

            // TODO_MA 注释： 如果从 SlotPool 中找到了未使用的 slot
            SlotAndLocality poolSlotAndLocality = optionalPoolSlotAndLocality.get();

            // TODO_MA 注释： 如果未使用的 AllocatedSlot 符合 Locality 偏好，或者前一步没有找到可用的 MultiTaskSlot
            if(poolSlotAndLocality.getLocality() == Locality.LOCAL || bestResolvedRootSlotWithLocality == null) {

                // TODO_MA 注释： 基于 新分配的 AllocatedSlot 创建一个 root MultiTaskSlot
                final PhysicalSlot allocatedSlot = poolSlotAndLocality.getSlot();
                final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager
                        .createRootSlot(multiTaskSlotRequestId, CompletableFuture.completedFuture(poolSlotAndLocality.getSlot()), allocatedSlotRequestId);

                // TODO_MA 注释： 将新创建的 root MultiTaskSlot 作为 AllocatedSlot 的 payload
                if(allocatedSlot.tryAssignPayload(multiTaskSlot)) {
                    return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, poolSlotAndLocality.getLocality());
                } else {
                    multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.'));
                }
            }
        }

        // TODO_MA 注释： 如果都不符合 Locality 偏好，或者 SlotPool 中没有可用的 slot 了
        if(multiTaskSlotLocality != null) {
            // prefer slot sharing group slots over unused slots
            if(optionalPoolSlotAndLocality.isPresent()) {
                slotPool.releaseSlot(allocatedSlotRequestId, new FlinkException("Locality constraint is not better fulfilled by allocated slot."));
            }
            return multiTaskSlotLocality;
        }

        // TODO_MA 注释： 先检查 slotSharingManager 中是不是还有没完成 slot 分配的 root MultiTaskSlot
        // there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
        SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.getUnresolvedRootSlot(groupId);

        if(multiTaskSlot == null) {
            // it seems as if we have to request a new slot from the resource manager, this is
            // always the last resort!!!

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 暂时没有可用的，如果允许排队的话，可以要求 SlotPool 向 RM 申请一个新的 slot
             */
            final CompletableFuture<PhysicalSlot> slotAllocationFuture = requestNewAllocatedSlot(allocatedSlotRequestId, slotProfile, allocationTimeout);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 请求分配后，就是同样的流程的，创建一个 root MultiTaskSlot，并作为新分配的 AllocatedSlot 的负载
             */
            multiTaskSlot = slotSharingManager.createRootSlot(multiTaskSlotRequestId, slotAllocationFuture, allocatedSlotRequestId);

            slotAllocationFuture.whenComplete((PhysicalSlot allocatedSlot, Throwable throwable) -> {
                final SlotSharingManager.TaskSlot taskSlot = slotSharingManager.getTaskSlot(multiTaskSlotRequestId);

                if(taskSlot != null) {
                    // still valid
                    if(!(taskSlot instanceof SlotSharingManager.MultiTaskSlot) || throwable != null) {
                        taskSlot.release(throwable);
                    } else {
                        if(!allocatedSlot.tryAssignPayload(((SlotSharingManager.MultiTaskSlot) taskSlot))) {
                            taskSlot.release(new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.'));
                        }
                    }
                } else {
                    slotPool.releaseSlot(allocatedSlotRequestId, new FlinkException("Could not find task slot with " + multiTaskSlotRequestId + '.'));
                }
            });
        }

        return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.UNKNOWN);
    }

    private void releaseSharedSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull SlotSharingGroupId slotSharingGroupId, Throwable cause) {

        final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

        if(multiTaskSlotManager != null) {
            final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(slotRequestId);

            if(taskSlot != null) {
                taskSlot.release(cause);
            } else {
                log.debug("Could not find slot [{}] in slot sharing group {}. Ignoring release slot request.", slotRequestId, slotSharingGroupId);
            }
        } else {
            log.debug("Could not find slot sharing group {}. Ignoring release slot request.", slotSharingGroupId);
        }
    }

    @Override
    public boolean requiresPreviousExecutionGraphAllocations() {
        return slotSelectionStrategy instanceof PreviousAllocationSlotSelectionStrategy;
    }
}
