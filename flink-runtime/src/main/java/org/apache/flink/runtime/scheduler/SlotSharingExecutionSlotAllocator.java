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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator maintains a shared slot for each {@link ExecutionSlotSharingGroup}. It allocates
 * a physical slot for the shared slot and then allocates logical slots from it for scheduled tasks.
 * The physical slot is lazily allocated for a shared slot, upon any hosted subtask asking for the
 * shared slot. Each subsequent sharing subtask allocates a logical slot from the existing shared
 * slot. The shared/physical slot can be released only if all the requested logical slots are
 * released or canceled.
 */
class SlotSharingExecutionSlotAllocator implements ExecutionSlotAllocator {
    private static final Logger LOG = LoggerFactory.getLogger(SlotSharingExecutionSlotAllocator.class);

    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final SlotSharingStrategy slotSharingStrategy;

    private final Map<ExecutionSlotSharingGroup, SharedSlot> sharedSlots;

    private final SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory;

    private final PhysicalSlotRequestBulkChecker bulkChecker;

    private final Time allocationTimeout;

    private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

    SlotSharingExecutionSlotAllocator(PhysicalSlotProvider slotProvider, boolean slotWillBeOccupiedIndefinitely,
            SlotSharingStrategy slotSharingStrategy, SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory,
            PhysicalSlotRequestBulkChecker bulkChecker, Time allocationTimeout,
            Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.slotSharingStrategy = checkNotNull(slotSharingStrategy);
        this.sharedSlotProfileRetrieverFactory = checkNotNull(sharedSlotProfileRetrieverFactory);
        this.bulkChecker = checkNotNull(bulkChecker);
        this.allocationTimeout = checkNotNull(allocationTimeout);
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.sharedSlots = new IdentityHashMap<>();
    }

    /**
     * Creates logical {@link SlotExecutionVertexAssignment}s from physical shared slots.
     *
     * <p>The allocation has the following steps:
     *
     * <ol>
     *   <li>Map the executions to {@link ExecutionSlotSharingGroup}s using {@link
     *       SlotSharingStrategy}
     *   <li>Check which {@link ExecutionSlotSharingGroup}s already have shared slot
     *   <li>For all involved {@link ExecutionSlotSharingGroup}s which do not have a shared slot
     *       yet:
     *   <li>Create a {@link SlotProfile} future using {@link SharedSlotProfileRetriever} and then
     *   <li>Allocate a physical slot from the {@link PhysicalSlotProvider}
     *   <li>Create a shared slot based on the returned physical slot futures
     *   <li>Allocate logical slot futures for the executions from all corresponding shared slots.
     *   <li>If a physical slot request fails, associated logical slot requests are canceled within
     *       the shared slot
     *   <li>Generate {@link SlotExecutionVertexAssignment}s based on the logical slot futures and
     *       returns the results.
     * </ol>
     *
     * @param executionVertexSchedulingRequirements the requirements for scheduling the executions.
     */
    /**
     * 该方法最终返回：List<SlotExecutionVertexAssignment>
     * SlotExecutionVertexAssignment描述的是一个 顶点Task 和一个 Slot 的分派关系的映射。(task和slot是多对一的关系)
     * 该方法的目的就是为了生成顶点task和物理slot的映射关系，主要两件事：
     *  1、申请物理slot/共享slot
     *  2、根据共享slot得到逻辑slot,建立task和物理slot的映射。每个task顶点和物理slot建立映射就是逻辑slot。
     * @param executionVertexSchedulingRequirements The requirements for scheduling the executions.
     * @return
     */
    @Override
    public List<SlotExecutionVertexAssignment> allocateSlotsFor(
            List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
        List<ExecutionVertexID> executionVertexIds = executionVertexSchedulingRequirements.stream()
                .map(ExecutionVertexSchedulingRequirements::getExecutionVertexId).collect(Collectors.toList());

        SharedSlotProfileRetriever sharedSlotProfileRetriever = sharedSlotProfileRetrieverFactory
                .createFromBulk(new HashSet<>(executionVertexIds));

        Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup = executionVertexIds.stream()
                .collect(Collectors.groupingBy(slotSharingStrategy::getExecutionSlotSharingGroup));

        // TODO_MA 注释： 给每个组申请 slot，默认情况下，所有task属于同一个组.
        Map<ExecutionSlotSharingGroup, SharedSlot> slots = executionsByGroup.keySet().stream()

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 申请 共享 slot
                 */
                .map(group -> getOrAllocateSharedSlot(group, sharedSlotProfileRetriever))
                .collect(Collectors.toMap(SharedSlot::getExecutionSlotSharingGroup, Function.identity()));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        //从物理/共享slot得到逻辑slot
        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = allocateLogicalSlotsFromSharedSlots(slots,
                executionsByGroup);

        // we need to pass the slots map to the createBulk method instead of using the allocator's
        // 'sharedSlots'
        // because if any physical slots have already failed, their shared slots have been removed
        // from the allocator's 'sharedSlots' by failed logical slots.
        SharingPhysicalSlotRequestBulk bulk = createBulk(slots, executionsByGroup);
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, allocationTimeout);

        return executionVertexIds.stream().map(assignments::get).collect(Collectors.toList());
    }

    @Override
    public void cancel(ExecutionVertexID executionVertexId) {
        cancelLogicalSlotRequest(executionVertexId, null);
    }

    private void cancelLogicalSlotRequest(ExecutionVertexID executionVertexId, Throwable cause) {
        ExecutionSlotSharingGroup executionSlotSharingGroup = slotSharingStrategy
                .getExecutionSlotSharingGroup(executionVertexId);
        checkNotNull(executionSlotSharingGroup,
                "There is no ExecutionSlotSharingGroup for ExecutionVertexID " + executionVertexId);
        SharedSlot slot = sharedSlots.get(executionSlotSharingGroup);
        if(slot != null) {
            slot.cancelLogicalSlotRequest(executionVertexId, cause);
        } else {
            LOG.debug("There is no SharedSlot for ExecutionSlotSharingGroup of ExecutionVertexID {}", executionVertexId);
        }
    }

    private static Map<ExecutionVertexID, SlotExecutionVertexAssignment> allocateLogicalSlotsFromSharedSlots(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup) {

        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = new HashMap<>();

        for(Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>> entry : executionsByGroup.entrySet()) {
            ExecutionSlotSharingGroup group = entry.getKey();
            List<ExecutionVertexID> executionIds = entry.getValue();

            // TODO_MA 注释： 遍历每个 ExecutionVertex 申请 LogicalSlot
            for(ExecutionVertexID executionId : executionIds) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                CompletableFuture<LogicalSlot> logicalSlotFuture = slots.get(group).allocateLogicalSlot(executionId);
                SlotExecutionVertexAssignment assignment = new SlotExecutionVertexAssignment(executionId,
                        logicalSlotFuture);
                assignments.put(executionId, assignment);
            }
        }

        return assignments;
    }

    private SharedSlot getOrAllocateSharedSlot(ExecutionSlotSharingGroup executionSlotSharingGroup,
            SharedSlotProfileRetriever sharedSlotProfileRetriever) {
        return sharedSlots.computeIfAbsent(executionSlotSharingGroup, group -> {
            SlotRequestId physicalSlotRequestId = new SlotRequestId();
            ResourceProfile physicalSlotResourceProfile = getPhysicalSlotResourceProfile(group);

            // TODO_MA 注释： 关于 ResourceProfile
            // TODO_MA 注释： 这个东西叫做资源配置：
            SlotProfile slotProfile = sharedSlotProfileRetriever.getSlotProfile(group, physicalSlotResourceProfile);
            PhysicalSlotRequest physicalSlotRequest = new PhysicalSlotRequest(physicalSlotRequestId, slotProfile,
                    slotWillBeOccupiedIndefinitely);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            CompletableFuture<PhysicalSlot> physicalSlotFuture = slotProvider.allocatePhysicalSlot(physicalSlotRequest)
                    .thenApply(PhysicalSlotRequest.Result::getPhysicalSlot);

            return new SharedSlot(physicalSlotRequestId, physicalSlotResourceProfile, group, physicalSlotFuture,
                    slotWillBeOccupiedIndefinitely, this::releaseSharedSlot);
        });
    }

    private void releaseSharedSlot(ExecutionSlotSharingGroup executionSlotSharingGroup) {
        SharedSlot slot = sharedSlots.remove(executionSlotSharingGroup);
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(slot.isEmpty(),
                "Trying to remove a shared slot with physical request id %s which has assigned logical slots",
                slot.getPhysicalSlotRequestId());
        slotProvider.cancelSlotRequest(slot.getPhysicalSlotRequestId(),
                new FlinkException("Slot is being returned from SlotSharingExecutionSlotAllocator."));
    }

    private ResourceProfile getPhysicalSlotResourceProfile(ExecutionSlotSharingGroup executionSlotSharingGroup) {
        return executionSlotSharingGroup.getExecutionVertexIds().stream()
                .reduce(ResourceProfile.ZERO, (r, e) -> r.merge(resourceProfileRetriever.apply(e)),
                        ResourceProfile::merge);
    }

    private SharingPhysicalSlotRequestBulk createBulk(Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions) {
        Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests = executions.keySet().stream()
                .collect(Collectors.toMap(group -> group, group -> slots.get(group).getPhysicalSlotResourceProfile()));
        SharingPhysicalSlotRequestBulk bulk = new SharingPhysicalSlotRequestBulk(executions, pendingRequests,
                this::cancelLogicalSlotRequest);
        registerPhysicalSlotRequestBulkCallbacks(slots, executions.keySet(), bulk);
        return bulk;
    }

    private static void registerPhysicalSlotRequestBulkCallbacks(Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Iterable<ExecutionSlotSharingGroup> executions, SharingPhysicalSlotRequestBulk bulk) {
        for(ExecutionSlotSharingGroup group : executions) {
            CompletableFuture<PhysicalSlot> slotContextFuture = slots.get(group).getSlotContextFuture();
            slotContextFuture.thenAccept(physicalSlot -> bulk.markFulfilled(group, physicalSlot.getAllocationId()));
            slotContextFuture.exceptionally(t -> {
                // clear the bulk to stop the fulfillability check
                bulk.clearPendingRequests();
                return null;
            });
        }
    }
}
