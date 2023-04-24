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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TaskManagerExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode. It
 * constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler, AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

    private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

    private static final int STARTUP_FAILURE_RETURN_CODE = 1;

    @VisibleForTesting
    static final int RUNTIME_FAILURE_RETURN_CODE = 2;

    private final Object lock = new Object();

    private final Configuration configuration;

    private final ResourceID resourceId;

    private final Time timeout;

    private final RpcService rpcService;

    private final HighAvailabilityServices highAvailabilityServices;

    private final MetricRegistryImpl metricRegistry;

    private final BlobCacheService blobCacheService;

    /**
     * Executor used to run future callbacks.
     */
    private final ExecutorService executor;

    private final TaskExecutorService taskExecutorService;

    private final CompletableFuture<Void> terminationFuture;

    private boolean shutdown;

    public TaskManagerRunner(Configuration configuration, PluginManager pluginManager, TaskExecutorServiceFactory taskExecutorServiceFactory) throws Exception {
        this.configuration = checkNotNull(configuration);

        timeout = AkkaUtils.getTimeoutAsTime(configuration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： TaskManager 内部线程池的数量和 物理cpu core个数 一样
         */
        this.executor = java.util.concurrent.Executors.newScheduledThreadPool(Hardware.getNumberCPUCores(), new ExecutorThreadFactory("taskmanager-future"));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 高可用服务
         */
        highAvailabilityServices = HighAvailabilityServicesUtils
                .createHighAvailabilityServices(configuration, executor, HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        rpcService = createRpcService(configuration, highAvailabilityServices);

        // TODO_MA 注释： 为 TaskManager 生成一个 ReousrceID
        this.resourceId = getTaskManagerResourceID(configuration, rpcService.getAddress(), rpcService.getPort());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化心跳服务，主要是初始化 心跳间隔 和 心跳超时 两个配置参数
         */
        HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 性能监控服务
         */
        metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration),
                ReporterSetup.fromConfiguration(configuration, pluginManager));

        final RpcService metricQueryServiceRpcService = MetricUtils.startRemoteMetricsRpcService(configuration, rpcService.getAddress());
        metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、在主节点启动的时候，事实上，已经启动了一个 BlobServer 的服务
         *  2、从节点启动的时候，会启动一个 BlobCacheService
         *  做文件缓存的服务
         */
        blobCacheService = new BlobCacheService(configuration, highAvailabilityServices.createBlobStore(), null);

        final ExternalResourceInfoProvider externalResourceInfoProvider = ExternalResourceUtils
                .createStaticExternalResourceInfoProvider(ExternalResourceUtils.getExternalResourceAmountMap(configuration),
                        ExternalResourceUtils.externalResourceDriversFromConfig(configuration, pluginManager));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建得到一个 TaskExecutorToServiceAdapter
         *  内部封装了 TaskExecutor，同时，TaskExecutor 的创建，就是在这内部完成的
         */
        taskExecutorService = taskExecutorServiceFactory
                .createTaskExecutor(this.configuration, this.resourceId, rpcService, highAvailabilityServices, heartbeatServices, metricRegistry,
                        blobCacheService, false, externalResourceInfoProvider, this);

        this.terminationFuture = new CompletableFuture<>();
        this.shutdown = false;

        handleUnexpectedTaskExecutorServiceTermination();

        MemoryLogger.startIfConfigured(LOG, configuration, terminationFuture);
    }

    private void handleUnexpectedTaskExecutorServiceTermination() {
        taskExecutorService.getTerminationFuture().whenComplete((unused, throwable) -> {
            synchronized(lock) {
                if(!shutdown) {
                    onFatalError(new FlinkException("Unexpected termination of the TaskExecutor.", throwable));
                }
            }
        });
    }

    // --------------------------------------------------------------------------------------------
    //  Lifecycle management
    // --------------------------------------------------------------------------------------------

    public void start() throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        taskExecutorService.start();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized(lock) {
            if(!shutdown) {
                shutdown = true;

                final CompletableFuture<Void> taskManagerTerminationFuture = taskExecutorService.closeAsync();

                final CompletableFuture<Void> serviceTerminationFuture = FutureUtils.composeAfterwards(taskManagerTerminationFuture, this::shutDownServices);

                serviceTerminationFuture.whenComplete((Void ignored, Throwable throwable) -> {
                    if(throwable != null) {
                        terminationFuture.completeExceptionally(throwable);
                    } else {
                        terminationFuture.complete(null);
                    }
                });
            }
        }

        return terminationFuture;
    }

    private CompletableFuture<Void> shutDownServices() {
        synchronized(lock) {
            Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
            Exception exception = null;

            try {
                JMXService.stopInstance();
            } catch(Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                blobCacheService.close();
            } catch(Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                metricRegistry.shutdown();
            } catch(Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                highAvailabilityServices.close();
            } catch(Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            terminationFutures.add(rpcService.stopService());

            terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

            if(exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    // export the termination future for caller to know it is terminated
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  FatalErrorHandler methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void onFatalError(Throwable exception) {
        TaskManagerExceptionUtils.tryEnrichTaskManagerError(exception);
        LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 关闭 JVM
         */
        // In case of the Metaspace OutOfMemoryError, we expect that the graceful shutdown is possible,
        // as it does not usually require more class loading to fail again with the Metaspace OutOfMemoryError.
        if(ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception) && !ExceptionUtils.isMetaspaceOutOfMemoryError(exception)) {
            terminateJVM();
        } else {
            closeAsync();
            FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            terminationFuture.whenComplete((Void ignored, Throwable throwable) -> terminateJVM());
        }
    }

    private void terminateJVM() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 关闭 JVM
         */
        System.exit(RUNTIME_FAILURE_RETURN_CODE);
    }

    // --------------------------------------------------------------------------------------------
    //  Static entry point
    // --------------------------------------------------------------------------------------------

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    public static void main(String[] args) throws Exception {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);
        long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

        if(maxOpenFileHandles != -1L) {
            LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
        } else {
            LOG.info("Cannot determine the maximum number of open file descriptors");
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 主入口
         */
        runTaskManagerSecurely(args);
    }

    public static Configuration loadConfiguration(String[] args) throws FlinkParseException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return ConfigurationParserUtils.loadCommonConfiguration(args, TaskManagerRunner.class.getSimpleName());
    }

    public static void runTaskManager(Configuration configuration, PluginManager pluginManager) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 构建一个 TaskManagerRunner 对象
         *  最重要的事情做了两件事：
         *  1、初始化了一个 TaskManagerServices 对象，其实就是包装了其他的各种服务
         *  2、创建了一个 TaskExecutor 对象
         *  -
         *  看起来是在创建 TaskManagerRunner ，但是做了两件事：
         *  1、new TaskManagerRunner(） 初始化了各种基础服务
         *  2、TaskManagerRunner::createTaskExecutorService 创建 TaskExecutor
         */
        final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, pluginManager,

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 重点在这里
                 *  这句代码就是真正去创建 TaskExecutor 的地方!
                 */
                TaskManagerRunner::createTaskExecutorService);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 然后启动
         *  然后跳转到 TaskExecutor 中的 onStart() 方法
         *  -
         *  做了一件最重要的事情： taskExecutor.start();
         */
        taskManagerRunner.start();
    }

    public static void runTaskManagerSecurely(String[] args) {
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 解析 args 和 flink-conf.yaml 得到配置信息
             */
            Configuration configuration = loadConfiguration(args);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动
             */
            runTaskManagerSecurely(configuration);

        } catch(Throwable t) {
            final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            LOG.error("TaskManager initialization failed.", strippedThrowable);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }
    }

    public static void runTaskManagerSecurely(Configuration configuration) throws Exception {
        replaceGracefulExitWithHaltIfConfigured(configuration);
        final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        FileSystem.initialize(configuration, pluginManager);
        SecurityUtils.install(new SecurityConfiguration(configuration));

        SecurityUtils.getInstalledContext().runSecured(() -> {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 运行 TaskManager
             */
            runTaskManager(configuration, pluginManager);
            return null;
        });
    }

    // --------------------------------------------------------------------------------------------
    //  Static utilities
    // --------------------------------------------------------------------------------------------

    public static TaskExecutorService createTaskExecutorService(Configuration configuration, ResourceID resourceID, RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, MetricRegistry metricRegistry,
            BlobCacheService blobCacheService, boolean localCommunicationOnly, ExternalResourceInfoProvider externalResourceInfoProvider,
            FatalErrorHandler fatalErrorHandler) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建 TaskExecutor
         */
        final TaskExecutor taskExecutor = startTaskManager(configuration, resourceID, rpcService, highAvailabilityServices, heartbeatServices, metricRegistry,
                blobCacheService, localCommunicationOnly, externalResourceInfoProvider, fatalErrorHandler);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  爷爷： TaskManagerRunner
         *  爸爸： TaskExecutorToServiceAdapter
         *  儿子： TaskExecutor
         */
        return TaskExecutorToServiceAdapter.createFor(taskExecutor);
    }

    public static TaskExecutor startTaskManager(Configuration configuration, ResourceID resourceID, RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, MetricRegistry metricRegistry,
            BlobCacheService blobCacheService, boolean localCommunicationOnly, ExternalResourceInfoProvider externalResourceInfoProvider,
            FatalErrorHandler fatalErrorHandler) throws Exception {

        checkNotNull(configuration);
        checkNotNull(resourceID);
        checkNotNull(rpcService);
        checkNotNull(highAvailabilityServices);

        LOG.info("Starting TaskManager with ResourceID: {}", resourceID.getStringWithMetadata());

        String externalAddress = rpcService.getAddress();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化资源配置
         */
        final TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 解析得到各种配置
         */
        TaskManagerServicesConfiguration taskManagerServicesConfiguration = TaskManagerServicesConfiguration
                .fromConfiguration(configuration, resourceID, externalAddress, localCommunicationOnly, taskExecutorResourceSpec);

        Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils
                .instantiateTaskManagerMetricGroup(metricRegistry, externalAddress, resourceID,
                        taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 4 倍 cpu core 个数
         */
        final ExecutorService ioExecutor = Executors
                .newFixedThreadPool(taskManagerServicesConfiguration.getNumIoThreads(), new ExecutorThreadFactory("flink-taskexecutor-io"));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一件重要的事情：
         *      初始化一个 TaskManagerServices，其实就是包装了其他各种服务
         *  内部实现中，其实是初始化了很多重要的基础服务
         *  但是一定要注释这两个：
         *  1、ShuffleEnvironment
         *  2、TaskSlotTable
         */
        TaskManagerServices taskManagerServices = TaskManagerServices
                .fromConfiguration(taskManagerServicesConfiguration, blobCacheService.getPermanentBlobService(), taskManagerMetricGroup.f1, ioExecutor,
                        fatalErrorHandler);

        MetricUtils.instantiateFlinkMemoryMetricGroup(taskManagerMetricGroup.f1, taskManagerServices.getTaskSlotTable(),
                taskManagerServices::getManagedMemorySize);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration
                .fromConfiguration(configuration, taskExecutorResourceSpec, externalAddress);

        String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第二件事： 创建 TaskExecutor
         */
        return new TaskExecutor(
                rpcService,
                taskManagerConfiguration,
                highAvailabilityServices,
                taskManagerServices,
                externalResourceInfoProvider,
                heartbeatServices,
                taskManagerMetricGroup.f0, metricQueryServiceAddress,
                blobCacheService,
                fatalErrorHandler,
                new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
                createBackPressureSampleService(configuration, rpcService.getScheduledExecutor())
        );
    }

    static BackPressureSampleService createBackPressureSampleService(Configuration configuration, ScheduledExecutor scheduledExecutor) {
        return new BackPressureSampleService(configuration.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES),
                Time.milliseconds(configuration.getInteger(WebOptions.BACKPRESSURE_DELAY)), scheduledExecutor);
    }

    /**
     * Create a RPC service for the task manager.
     *
     * @param configuration The configuration for the TaskManager.
     * @param haServices    to use for the task manager hostname retrieval
     */
    @VisibleForTesting
    static RpcService createRpcService(final Configuration configuration, final HighAvailabilityServices haServices) throws Exception {

        checkNotNull(configuration);
        checkNotNull(haServices);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return AkkaRpcServiceUtils.createRemoteRpcService(configuration, determineTaskManagerBindAddress(configuration, haServices),
                configuration.getString(TaskManagerOptions.RPC_PORT), configuration.getString(TaskManagerOptions.BIND_HOST),
                configuration.getOptional(TaskManagerOptions.RPC_BIND_PORT));
    }

    private static String determineTaskManagerBindAddress(final Configuration configuration, final HighAvailabilityServices haServices) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 从配置中获取 taskmanager.host
         */
        final String configuredTaskManagerHostname = configuration.getString(TaskManagerOptions.HOST);

        // TODO_MA 注释： 如果配置了 hostname/address 则使用配置的。
        if(configuredTaskManagerHostname != null) {
            LOG.info("Using configured hostname/address for TaskManager: {}.", configuredTaskManagerHostname);
            return configuredTaskManagerHostname;
        } else {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 否则自行决定
             */
            return determineTaskManagerBindAddressByConnectingToResourceManager(configuration, haServices);
        }
    }

    private static String determineTaskManagerBindAddressByConnectingToResourceManager(final Configuration configuration,
            final HighAvailabilityServices haServices) throws LeaderRetrievalException {

        final Duration lookupTimeout = AkkaUtils.getLookupTimeout(configuration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(haServices.getResourceManagerLeaderRetriever(), lookupTimeout);

        LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.", taskManagerAddress.getHostName(), taskManagerAddress.getHostAddress());

        HostBindPolicy bindPolicy = HostBindPolicy.fromString(configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
        return bindPolicy == HostBindPolicy.IP ? taskManagerAddress.getHostAddress() : taskManagerAddress.getHostName();
    }

    @VisibleForTesting
    static ResourceID getTaskManagerResourceID(Configuration config, String rpcAddress, int rpcPort) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new ResourceID(config.getString(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID,
                StringUtils.isNullOrWhitespaceOnly(rpcAddress) ? InetAddress.getLocalHost().getHostName() + "-" + new AbstractID().toString()
                        .substring(0, 6) : rpcAddress + ":" + rpcPort + "-" + new AbstractID().toString().substring(0, 6)),
                config.getString(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, ""));
    }

    /**
     * Factory for {@link TaskExecutor}.
     */
    public interface TaskExecutorServiceFactory {
        TaskExecutorService createTaskExecutor(Configuration configuration, ResourceID resourceID, RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, MetricRegistry metricRegistry,
                BlobCacheService blobCacheService, boolean localCommunicationOnly, ExternalResourceInfoProvider externalResourceInfoProvider,
                FatalErrorHandler fatalErrorHandler) throws Exception;
    }

    public interface TaskExecutorService extends AutoCloseableAsync {
        void start();

        CompletableFuture<Void> getTerminationFuture();
    }
}
