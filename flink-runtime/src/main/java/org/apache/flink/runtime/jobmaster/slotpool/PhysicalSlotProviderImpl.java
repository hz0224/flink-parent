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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The provider serves physical slot requests.
 */
public class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);

    private final SlotSelectionStrategy slotSelectionStrategy;

    private final SlotPool slotPool;

    public PhysicalSlotProviderImpl(SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
        this.slotPool = checkNotNull(slotPool);
        slotPool.disableBatchSlotRequestTimeoutCheck();
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     *  关于 Allocator 其实有两种实现：
     *  1、SlotShare
     *  2、Default
     *  申请物理slot的最终实现！
     *  -
     *  内部分为两个步骤：
     *  1、tryAllocateFromAvailable(slotRequestId, slotProfile);
     *      从可用的里面去申请
     *      如果你总共需要申请10个，现在已经申请到了 8 个，正在申请第9个，那么第九个，可能会从 前面申请到的这 8 个进行分配
     *      检查是否满足要求，如果满足，可以进行slotshare
     *  2、requestNewSlot(slotRequestId, resourceProfile, physicalSlotRequest.willSlotBeOccupiedIndefinitely())
     *      发送 RPC 请求问 ResourceManager 再申请一个
     */
    @Override
    public CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(PhysicalSlotRequest physicalSlotRequest) {
        SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
        SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
        ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

        LOG.debug("Received slot request [{}] with resource requirements: {}", slotRequestId, resourceProfile);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        Optional<PhysicalSlot> availablePhysicalSlot = tryAllocateFromAvailable(slotRequestId, slotProfile);

        CompletableFuture<PhysicalSlot> slotFuture;
        slotFuture = availablePhysicalSlot.map(CompletableFuture::completedFuture).orElseGet(

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 发送 申请 slot 的 rpc 请求给 RM
                 */
                () -> requestNewSlot(slotRequestId, resourceProfile,
                        physicalSlotRequest.willSlotBeOccupiedIndefinitely()));

        return slotFuture.thenApply(physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
    }

    private Optional<PhysicalSlot> tryAllocateFromAvailable(SlotRequestId slotRequestId, SlotProfile slotProfile) {
        Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList = slotPool.getAvailableSlotsInformation()
                .stream().map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot).collect(Collectors.toList());

        Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot = slotSelectionStrategy
                .selectBestSlotForProfile(slotInfoList, slotProfile);

        // TODO_MA 注释： 一切跟 slot 管理有关的事情，都是由  SlotPool
        return selectedAvailableSlot.flatMap(slotInfoAndLocality -> slotPool
                .allocateAvailableSlot(slotRequestId, slotInfoAndLocality.getSlotInfo().getAllocationId()));
    }

    private CompletableFuture<PhysicalSlot> requestNewSlot(SlotRequestId slotRequestId, ResourceProfile resourceProfile,
            boolean willSlotBeOccupiedIndefinitely) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： willSlotBeOccupiedIndefinitely = true
         */
        if(willSlotBeOccupiedIndefinitely) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            return slotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, null);
        } else {

            // TODO_MA 注释： 批处理申请
            return slotPool.requestNewAllocatedBatchSlot(slotRequestId, resourceProfile);
        }
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
        slotPool.releaseSlot(slotRequestId, cause);
    }
}
