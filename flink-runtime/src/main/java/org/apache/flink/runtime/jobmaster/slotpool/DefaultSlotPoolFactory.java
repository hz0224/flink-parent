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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.clock.Clock;

import javax.annotation.Nonnull;

/** Default slot pool factory. */
public class DefaultSlotPoolFactory extends AbstractSlotPoolFactory {

    public DefaultSlotPoolFactory(
            @Nonnull Clock clock,
            @Nonnull Time rpcTimeout,
            @Nonnull Time slotIdleTimeout,
            @Nonnull Time batchSlotTimeout) {
        super(clock, rpcTimeout, slotIdleTimeout, batchSlotTimeout);
    }

    @Override
    @Nonnull
    public SlotPool createSlotPool(@Nonnull JobID jobId) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： SlotPool 的实现
         */
        return new SlotPoolImpl(jobId, clock, rpcTimeout, slotIdleTimeout, batchSlotTimeout);
    }
}
