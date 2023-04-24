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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobGraphStoreFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent}
 * components.
 */
public class DefaultDispatcherResourceManagerComponentFactory implements DispatcherResourceManagerComponentFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nonnull
    private final DispatcherRunnerFactory dispatcherRunnerFactory;

    @Nonnull
    private final ResourceManagerFactory<?> resourceManagerFactory;

    @Nonnull
    private final RestEndpointFactory<?> restEndpointFactory;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： ComponentFactory 的内部，其实就是包含了。要创建
     *  ResourceManager Dispatcher WebMonitorEndpoint 三大组件的工厂实例
     */
    public DefaultDispatcherResourceManagerComponentFactory(@Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
            @Nonnull ResourceManagerFactory<?> resourceManagerFactory, @Nonnull RestEndpointFactory<?> restEndpointFactory) {
        this.dispatcherRunnerFactory = dispatcherRunnerFactory;
        this.resourceManagerFactory = resourceManagerFactory;
        this.restEndpointFactory = restEndpointFactory;
    }

    @Override
    public DispatcherResourceManagerComponent create(Configuration configuration, Executor ioExecutor, RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices, BlobServer blobServer, HeartbeatServices heartbeatServices, MetricRegistry metricRegistry,
            ArchivedExecutionGraphStore archivedExecutionGraphStore, MetricQueryServiceRetriever metricQueryServiceRetriever,
            FatalErrorHandler fatalErrorHandler) throws Exception {

        LeaderRetrievalService dispatcherLeaderRetrievalService = null;
        LeaderRetrievalService resourceManagerRetrievalService = null;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 最重要的就是这三个组件！
         */
        WebMonitorEndpoint<?> webMonitorEndpoint = null;
        ResourceManager<?> resourceManager = null;
        DispatcherRunner dispatcherRunner = null;

        try {

            // TODO_MA 注释： DefaultLeaderRetrievalService 监听 Dispatcher 地址
            dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

            // TODO_MA 注释： DefaultLeaderRetrievalService 监听 ResourceManager 地址
            resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

            // TODO_MA 注释： Dispatcher 的 GatewayRetriever
            final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(rpcService, DispatcherGateway.class,
                    DispatcherId::fromUuid, new ExponentialBackoffRetryStrategy(12, Duration.ofMillis(10), Duration.ofMillis(50)));

            // TODO_MA 注释： ResourceManager 的 GatewayRetriever
            final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(rpcService,
                    ResourceManagerGateway.class, ResourceManagerId::fromUuid,
                    new ExponentialBackoffRetryStrategy(12, Duration.ofMillis(10), Duration.ofMillis(50)));

            // TODO_MA 注释： 创建线程池，用于执行 WebMonitorEndpoint 所接收到的 client 发送过来的请求
            final ScheduledExecutorService executor = WebMonitorEndpoint.createExecutorService(
                    configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
                    configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
                    "DispatcherRestEndpoint"
            );

            // TODO_MA 注释： 初始化 MetricFetcher，间隔时间是：10s
            final long updateInterval = configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
            final MetricFetcher metricFetcher = updateInterval == 0 ? VoidMetricFetcher.INSTANCE : MetricFetcherImpl
                    .fromConfiguration(configuration, metricQueryServiceRetriever, dispatcherGatewayRetriever, executor);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 WebMonitorEndpoint 实例， 在 Standalone 模式下： DispatcherRestEndpoint
             *  1、restEndpointFactory = SessionRestEndpointFactory
             *  2、webMonitorEndpoint = DispatcherRestEndpoint
             *  当前这个 DispatcherRestEndpoint 的作用是：
             *  1、初始化的过程中，会初始化一大堆的 Handler
             *  2、启动一个 Netty 的服务端，绑定了这些 注册和排序这些 Handler
             *  3、当 client 通过 flink 命令提交了某些操作（发起 restful 请求），服务端由 webMonitorEndpoint 来执行处理
             *  -
             *  举个例子：
             *  如果用户通过 flink run 提交一个 Job，那么最后是由 WebMonitorEndpoint 中的 JobSubmitHandler 来执行处理
             *  job 由 JobSubmitHandler 执行完毕之后，转交给 Dispatcher 去调度执行
             *  -
             *  如果是 YARN per-job 模式，则 webMonitorEndpoint = MiniDispatcherRestEndpoint
             *  -
             *  其实真正要创建的是一个 WebMonitorEndpoint， 它是 RestServerEndpoint 的实现类
             */
            webMonitorEndpoint = restEndpointFactory
                    .createRestEndpoint(configuration, dispatcherGatewayRetriever, resourceManagerGatewayRetriever, blobServer, executor, metricFetcher,
                            highAvailabilityServices.getClusterRestEndpointLeaderElectionService(), fatalErrorHandler);

            log.debug("Starting Dispatcher REST endpoint.");
            webMonitorEndpoint.start();

            final String hostname = RpcUtils.getHostname(rpcService);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  其实三个要点：
             *  1、ResourceManager 是一个 RpcEndpoint，当构建好了这个对象之后调用 start()，去看它的 onStart() 即可
             *  2、ResourceManager 也是一个 LeaderContender，也会执行竞选，竞选处理
             *  3、启动 ResourceManager Service
             *      两个心跳服务
             *          从节点 和 主节点之间的心跳
             *          Job的主控程序 和 主节点之间的心跳
             *      两个定时服务
             *          TaskManager 的超时检查服务
             *          Slot申请的 超时检查服务
             */
            resourceManager = resourceManagerFactory
                    .createResourceManager(configuration, ResourceID.generate(), rpcService, highAvailabilityServices, heartbeatServices, fatalErrorHandler,
                            new ClusterInformation(hostname, blobServer.getPort()), webMonitorEndpoint.getRestBaseUrl(), metricRegistry, hostname, ioExecutor);

            final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist
                    .createHistoryServerArchivist(configuration, webMonitorEndpoint, ioExecutor);

            // TODO_MA 注释： 创建 PartialDispatcherServices
            final PartialDispatcherServices partialDispatcherServices = new PartialDispatcherServices(configuration, highAvailabilityServices,
                    resourceManagerGatewayRetriever, blobServer, heartbeatServices,
                    () -> MetricUtils.instantiateJobManagerMetricGroup(metricRegistry, hostname), archivedExecutionGraphStore, fatalErrorHandler,
                    historyServerArchivist, metricRegistry.getMetricQueryServiceGatewayRpcAddress(), ioExecutor);

            log.debug("Starting Dispatcher.");

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             *  1、在该代码的内部，会创建 Dispatcher 组件
             *  2、调用 start() 启动
             *  -
             *  找不到 dispatcherRunner.start() 这句代码的
             *  其实是 dispatcherRunner 的内部，封装了 Dispatcher
             *  在该 create 方法的内部，回去创建 Dispatcher 并调用 start() 来启动
             */
            dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(highAvailabilityServices.getDispatcherLeaderElectionService(), fatalErrorHandler,
                    new HaServicesJobGraphStoreFactory(highAvailabilityServices), ioExecutor, rpcService, partialDispatcherServices);

            // TODO_MA 注释： 调用 ResourceManager 的 start() 启动 Rpc 服务，则代码执行跳转到 onStart() 中
            log.debug("Starting ResourceManager.");
            resourceManager.start();

            resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
            dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            return new DispatcherResourceManagerComponent(dispatcherRunner, DefaultResourceManagerService.createFor(resourceManager),
                    dispatcherLeaderRetrievalService, resourceManagerRetrievalService, webMonitorEndpoint, fatalErrorHandler);

        } catch(Exception exception) {
            // clean up all started components
            if(dispatcherLeaderRetrievalService != null) {
                try {
                    dispatcherLeaderRetrievalService.stop();
                } catch(Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if(resourceManagerRetrievalService != null) {
                try {
                    resourceManagerRetrievalService.stop();
                } catch(Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if(webMonitorEndpoint != null) {
                terminationFutures.add(webMonitorEndpoint.closeAsync());
            }

            if(resourceManager != null) {
                terminationFutures.add(resourceManager.closeAsync());
            }

            if(dispatcherRunner != null) {
                terminationFutures.add(dispatcherRunner.closeAsync());
            }

            final FutureUtils.ConjunctFuture<Void> terminationFuture = FutureUtils.completeAll(terminationFutures);

            try {
                terminationFuture.get();
            } catch(Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            throw new FlinkException("Could not create the DispatcherResourceManagerComponent.", exception);
        }
    }

    public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(ResourceManagerFactory<?> resourceManagerFactory) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new DefaultDispatcherResourceManagerComponentFactory(

                // TODO_MA 注释： 第二个工厂： DefaultDispatcherRunnerFactory
                // TODO_MA 注释： 从来创建 Dispatcher
                DefaultDispatcherRunnerFactory.createSessionRunner(SessionDispatcherFactory.INSTANCE),

                // TODO_MA 注释： 第一个工厂： StandaloneResourceManagerFactory
                // TODO_MA 注释： 用来创建 ResourceManager
                resourceManagerFactory,

                // TODO_MA 注释： 第三个工厂： SessionRestEndpointFactory
                // TODO_MA 注释： 用来创建 WebMonitorEndpoint
                SessionRestEndpointFactory.INSTANCE
        );
    }

    public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(ResourceManagerFactory<?> resourceManagerFactory,
            JobGraphRetriever jobGraphRetriever) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new DefaultDispatcherResourceManagerComponentFactory(

                // TODO_MA 注释： 第二个工厂实例： DefaultDispatcherRunnerFactory
                DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),

                // TODO_MA 注释： 第一个工厂实例： YarnResourceManagerFactory
                resourceManagerFactory,

                // TODO_MA 注释： 第三个工厂实例： JobRestEndpointFactory
                JobRestEndpointFactory.INSTANCE
        );
    }
}
