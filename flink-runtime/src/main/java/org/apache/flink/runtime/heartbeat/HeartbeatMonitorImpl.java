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
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default implementation of {@link HeartbeatMonitor}.
 *
 * @param <O> Type of the payload being sent to the associated heartbeat target
 */
public class HeartbeatMonitorImpl<O> implements HeartbeatMonitor<O>, Runnable {

    /**
     * Resource ID of the monitored heartbeat target.
     */
    private final ResourceID resourceID;

    /**
     * Associated heartbeat target.
     */
    private final HeartbeatTarget<O> heartbeatTarget;

    private final ScheduledExecutor scheduledExecutor;

    /**
     * Listener which is notified about heartbeat timeouts.
     */
    private final HeartbeatListener<?, ?> heartbeatListener;

    /**
     * Maximum heartbeat timeout interval.
     */
    private final long heartbeatTimeoutIntervalMs;

    private volatile ScheduledFuture<?> futureTimeout;

    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    private volatile long lastHeartbeat;

    HeartbeatMonitorImpl(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget, ScheduledExecutor scheduledExecutor,
            HeartbeatListener<?, O> heartbeatListener, long heartbeatTimeoutIntervalMs) {

        this.resourceID = Preconditions.checkNotNull(resourceID);
        this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
        this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);

        Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout interval has to be larger than 0.");
        this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

        lastHeartbeat = 0L;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重置心跳超时机制
         */
        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public HeartbeatTarget<O> getHeartbeatTarget() {
        return heartbeatTarget;
    }

    @Override
    public ResourceID getHeartbeatTargetId() {
        return resourceID;
    }

    @Override
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    @Override
    public void reportHeartbeat() {

        // TODO_MA 注释： 记录最后一次的心跳时间
        lastHeartbeat = System.currentTimeMillis();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重置心跳超时
         *  只要有人提交了作业，我就重新计时。如果在某个指定时间范围内，没有人再提交的话，我就不要了。
         *  如果有人提交，我就重置这个时间：  StopWatch 秒表
         */
        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public void cancel() {
        // we can only cancel if we are in state running
        if(state.compareAndSet(State.RUNNING, State.CANCELED)) {
            cancelTimeout();
        }
    }

    @Override
    public void run() {
        // The heartbeat has timed out if we're in state running
        if(state.compareAndSet(State.RUNNING, State.TIMEOUT)) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 通知超时
             */
            heartbeatListener.notifyHeartbeatTimeout(resourceID);
        }
    }

    public boolean isCanceled() {
        return state.get() == State.CANCELED;
    }

    void resetHeartbeatTimeout(long heartbeatTimeout) {

        // TODO_MA 注释： 只有当 HeartbeatMonitor 的状态处于 Running 的时候
        if(state.get() == State.RUNNING) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 取消
             */
            cancelTimeout();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 重新调度，这是一种延时调度
             */
            futureTimeout = scheduledExecutor.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 双检查
             */
            // Double check for concurrent accesses (e.g. a firing of the scheduled future)
            if(state.get() != State.RUNNING) {
                cancelTimeout();
            }
        }
    }

    private void cancelTimeout() {
        if(futureTimeout != null) {
            futureTimeout.cancel(true);
        }
    }

    private enum State {
        RUNNING, TIMEOUT, CANCELED
    }

    /**
     * The factory that instantiates {@link HeartbeatMonitorImpl}.
     *
     * @param <O> Type of the outgoing heartbeat payload
     */
    static class Factory<O> implements HeartbeatMonitor.Factory<O> {

        @Override
        public HeartbeatMonitor<O> createHeartbeatMonitor(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget, ScheduledExecutor mainThreadExecutor,
                HeartbeatListener<?, O> heartbeatListener, long heartbeatTimeoutIntervalMs) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 将 HeartbeatTarget 封装成为 HeartbeatMonitor
             *  具体实现是： HeartbeatMonitorImpl
             */
            return new HeartbeatMonitorImpl<>(resourceID, heartbeatTarget, mainThreadExecutor, heartbeatListener, heartbeatTimeoutIntervalMs);
        }
    }
}
