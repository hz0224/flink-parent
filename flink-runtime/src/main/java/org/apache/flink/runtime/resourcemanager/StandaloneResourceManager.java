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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A standalone implementation of the resource manager. Used when the system is started in
 * standalone mode (via scripts), rather than via a resource framework like YARN or Mesos.
 *
 * <p>This ResourceManager doesn't acquire new resources.
 */
public class StandaloneResourceManager extends ResourceManager<ResourceID> {

    /**
     * The duration of the startup period. A duration of zero means there is no startup period.
     */
    private final Time startupPeriodTime;

    public StandaloneResourceManager(RpcService rpcService, ResourceID resourceId, HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices, SlotManager slotManager, ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            JobLeaderIdService jobLeaderIdService, ClusterInformation clusterInformation, FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup, Time startupPeriodTime, Time rpcTimeout, Executor ioExecutor) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 调用父类 RpcEndpoint 的构造器，创建 RpcServer
         */
        super(rpcService, resourceId, highAvailabilityServices, heartbeatServices, slotManager, clusterPartitionTrackerFactory, jobLeaderIdService,
                clusterInformation, fatalErrorHandler, resourceManagerMetricGroup, rpcTimeout, ioExecutor);

        this.startupPeriodTime = Preconditions.checkNotNull(startupPeriodTime);
    }

    @Override
    protected void initialize() throws ResourceManagerException {
        // nothing to initialize
    }

    @Override
    protected void terminate() {
        // noop
    }

    @Override
    protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) {
    }

    // TODO_MA 注释： 对于 Standalone 模式而言， TaskExecutor 数量和资源情况是固定的，不支持动态启动和释放
    @Override
    public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： Standalone 模式下，是不可以通过命令，多启动 TaskManager
         *  Standalone 模式下，除非手动运维，否则资源都是固定的
         */
        return false;
    }

    // TODO_MA 注释： 对于 Standalone 模式而言， TaskExecutor 数量和资源情况是固定的，不支持动态启动和释放
    @Override
    public boolean stopWorker(ResourceID resourceID) {
        // standalone resource manager cannot stop workers
        return false;
    }

    @Override
    protected ResourceID workerStarted(ResourceID resourceID) {
        return resourceID;
    }

    @Override
    protected void onLeadership() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 开启 启动期
         *  JobManager 主节点启动好了之后，如果过了一段时间，还是没有任何的 TaskManager 从节点过来注册。
         *  当 JobManager 启动好了，到 第一个 TaskManager 过来注册的这段时间。
         *  如果超过某个指定时间，意味着启动失败！
         */
        startStartupPeriod();
    }

    private void startStartupPeriod() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        setFailUnfulfillableRequest(false);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动定时任务
         */
        final long startupPeriodMillis = startupPeriodTime.toMilliseconds();
        if(startupPeriodMillis > 0) {
            scheduleRunAsync(() -> setFailUnfulfillableRequest(true), startupPeriodMillis, TimeUnit.MILLISECONDS);
        }
    }
}
