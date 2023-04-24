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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * {@link HeartbeatManager} implementation which regularly requests a heartbeat response from its
 * monitored {@link HeartbeatTarget}. The heartbeat period is configurable.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
public class HeartbeatManagerSenderImpl<I, O> extends HeartbeatManagerImpl<I, O> implements Runnable {

    private final long heartbeatPeriod;

    HeartbeatManagerSenderImpl(long heartbeatPeriod, long heartbeatTimeout, ResourceID ownResourceID, HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor, Logger log) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 调用重载构造
         */
        this(heartbeatPeriod, heartbeatTimeout, ownResourceID, heartbeatListener, mainThreadExecutor, log, new HeartbeatMonitorImpl.Factory<>());
    }

    HeartbeatManagerSenderImpl(long heartbeatPeriod, long heartbeatTimeout, ResourceID ownResourceID, HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor, Logger log, HeartbeatMonitor.Factory<O> heartbeatMonitorFactory) {
        super(heartbeatTimeout, ownResourceID, heartbeatListener, mainThreadExecutor, log, heartbeatMonitorFactory);

        this.heartbeatPeriod = heartbeatPeriod;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 延迟调度
         *  调用 this 的 run() 方法立即执行一次
         */
        mainThreadExecutor.schedule(this, 0L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        if(!stopped) {
            log.debug("Trigger heartbeat request.");

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  HeartbeatMonitor
             *  HeartbeatTarget
             *  举个例子： 相对于 ResourceManager 来说， JobMaster 是被动接受心跳请求的一方
             *  相对于 ResourceManager 来说， JobMaster 就是它的 heartBeatTarget
             */
            for(HeartbeatMonitor<O> heartbeatMonitor : getHeartbeatTargets().values()) {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 发送心跳 RPC 请求
                 */
                requestHeartbeat(heartbeatMonitor);
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 这句代码就实现 心跳 的无限循环！
             *  通俗解释： 延迟 10s 执行一次！
             */
            getMainThreadExecutor().schedule(this, heartbeatPeriod, TimeUnit.MILLISECONDS);
        }
    }

    private void requestHeartbeat(HeartbeatMonitor<O> heartbeatMonitor) {
        O payload = getHeartbeatListener().retrievePayload(heartbeatMonitor.getHeartbeatTargetId());
        final HeartbeatTarget<O> heartbeatTarget = heartbeatMonitor.getHeartbeatTarget();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 给心跳目标对象，发送心跳
         */
        heartbeatTarget.requestHeartbeat(getOwnResourceID(), payload);
    }
}
