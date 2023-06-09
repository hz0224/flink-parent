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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcMetricQueryServiceRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 * 1 ClusterEntrypoint代表集群的模式，它有3个子类：
 *      1 SessionClusterEntrypoint  session集群，可以基于Standalone，可以基于yarn，可以基于 mesos
 *          （1）StandaloneSessionClusterEntrypoint   独立的session集群，这也是Flink自带的集群启动入口点。
 *          （2）YarnSessionClusterEntrypoint         yarn session集群的启动入口点，借助yran实现的session集群，实现了资源的动态分配。
 *      2 JobClusterEntrypoint  per-job集群       可以基于yarn，可以基于Standalone
 *          （1）YarnJobClusterEntrypoint             yarn的per-job集群启动入口点。
 *      3 ApplicationClusterEntryPoint  application集群   可以基于yarn，可以基于Standalone
 *          （1）YarnApplicationClusterEntryPoint     yarn的application集群启动入口点
 *          （2）StandaloneApplicationClusterEntryPoint   独立的application集群启动入口点
 *
 * 2 DefaultDispatcherResourceManagerComponentFactory：初始化了一个 DefaultDispatcherResourceManagerComponentFactory 工厂实例，该类内部又会创建3个工厂实例：
 *      1、Dispatcher = DefaultDispatcherRunnerFactory，生产 DefaultDispatcherRunner， 具体实现是： DispatcherRunnerLeaderElectionLifecycleManager
 *      2、ResourceManager = StandaloneResourceManagerFactory，生产 StandaloneResourceManager
 *      3、WebMonitorEndpoint = SessionRestEndpointFactory，生产 DispatcherRestEndpoint
 * 这3个工厂实例用来创建 JobManager中启动过程中最重要的3个组件：
 *      （1）Dispatcher   调度任务
 *      （2）ResourceManager  资源管理
 *      （3）WebMonitorEndpoint   接受client提交的任务，接着提交给Dispatcher进行调度。
 * ClusterEntrypoin决定了启动的是一个什么样的集群，其内部会通过 工厂去创建ResourceManager实例，如果使用的是外部资源框架，那么该ResourceManager就是ActiveResourceManager类型。
 * ActiveResourceManager内部有一个ResourceManagerDriver类型的接口变量，代表具体的资源框架。
 * 因此ClusterEntrypoint决定了集群的模式， ClusterEntrypoint内的 ResourceManager中的ResourceManagerDriver决定了哪个资源框架。
 */
public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

    public static final ConfigOption<String> EXECUTION_MODE = ConfigOptions.key("internal.cluster.execution-mode")
            .defaultValue(ExecutionMode.NORMAL.toString());

    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

    protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
    protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

    private static final Time INITIALIZATION_SHUTDOWN_TIMEOUT = Time.seconds(30L);

    /**
     * The lock to guard startup / shutdown / manipulation methods.
     */
    private final Object lock = new Object();

    private final Configuration configuration;

    private final CompletableFuture<ApplicationStatus> terminationFuture;

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    @GuardedBy("lock")
    private DispatcherResourceManagerComponent clusterComponent;

    @GuardedBy("lock")
    private MetricRegistryImpl metricRegistry;

    @GuardedBy("lock")
    private ProcessMetricGroup processMetricGroup;

    @GuardedBy("lock")
    private HighAvailabilityServices haServices;

    @GuardedBy("lock")
    private BlobServer blobServer;

    @GuardedBy("lock")
    private HeartbeatServices heartbeatServices;

    @GuardedBy("lock")
    private RpcService commonRpcService;

    @GuardedBy("lock")
    private ExecutorService ioExecutor;

    private ArchivedExecutionGraphStore archivedExecutionGraphStore;

    private final Thread shutDownHook;

    protected ClusterEntrypoint(Configuration configuration) {
        this.configuration = generateClusterConfiguration(configuration);
        this.terminationFuture = new CompletableFuture<>();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 注册钩子，在出错，或者关闭的时候，删除 临时工作目录
         */
        shutDownHook = ShutdownHookUtil.addShutdownHook(this::cleanupDirectories, getClass().getSimpleName(), LOG);
    }

    public CompletableFuture<ApplicationStatus> getTerminationFuture() {
        return terminationFuture;
    }

    public void startCluster() throws ClusterEntrypointException {
        LOG.info("Starting {}.", getClass().getSimpleName());

        try {
            replaceGracefulExitWithHaltIfConfigured(configuration);
            PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
            configureFileSystems(configuration, pluginManager);
            SecurityContext securityContext = installSecurityContext(configuration);

            securityContext.runSecured((Callable<Void>) () -> {

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 继续启动
                 */
                runCluster(configuration, pluginManager);

                return null;
            });
        } catch(Throwable t) {
            final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);

            try {
                // clean up any partial state
                shutDownAsync(ApplicationStatus.FAILED, ExceptionUtils.stringifyException(strippedThrowable), false)
                        .get(INITIALIZATION_SHUTDOWN_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
            } catch(InterruptedException | ExecutionException | TimeoutException e) {
                strippedThrowable.addSuppressed(e);
            }

            throw new ClusterEntrypointException(String.format("Failed to initialize the cluster entrypoint %s.", getClass().getSimpleName()),
                    strippedThrowable);
        }
    }

    private void configureFileSystems(Configuration configuration, PluginManager pluginManager) {
        LOG.info("Install default filesystem.");

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        FileSystem.initialize(configuration, pluginManager);
    }

    private SecurityContext installSecurityContext(Configuration configuration) throws Exception {
        LOG.info("Install security context.");

        SecurityUtils.install(new SecurityConfiguration(configuration));

        return SecurityUtils.getInstalledContext();
    }

    private void runCluster(Configuration configuration, PluginManager pluginManager) throws Exception {
        synchronized(lock) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 初始化了主节点对外提供服务的时候所需要的三大核心组件启动时所需要的基础服务
             *  1、commonRpcService: 	基于 Akka 的 RpcService 实现。RPC 服务启动 Akka 参与者来接收从 RpcGateway 调用 RPC
             *  2、JMXService:          启动一个 JMXService
             *  3、ioExecutor:          启动一个线程池
             *  4、haServices: 			提供对高可用性所需的所有服务的访问注册，分布式计数器和领导人选举
             *  5、blobServer: 			负责侦听传入的请求生成线程来处理这些请求。它还负责创建要存储的目录结构 blob 或临时缓存它们
             *  6、heartbeatServices: 	提供心跳所需的所有服务。这包括创建心跳接收器和心跳发送者。
             *  7、metricRegistry:  	跟踪所有已注册的 Metric，它作为连接 MetricGroup 和 MetricReporter
             *  8、archivedExecutionGraphStore:  	存储执行图ExecutionGraph的可序列化形式。
             */
            initializeServices(configuration, pluginManager);

            // TODO_MA 注释： JobManager 默认端口：6123
            // write host information into configuration
            configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 以 Flink Standalone 模式为例子
             *  初始化一个 DefaultDispatcherResourceManagerComponentFactory 工厂实例
             *  内部初始化了四大工厂实例
             *  1、Dispatcher = DefaultDispatcherRunnerFactory，生产 DefaultDispatcherRunner， 具体实现是： DispatcherRunnerLeaderElectionLifecycleManager
             *  2、ResourceManager = StandaloneResourceManagerFactory，生产 StandaloneResourceManager
             *  3、WebMonitorEndpoint = SessionRestEndpointFactory，生产 DispatcherRestEndpoint
             */
            final DispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory =
                    createDispatcherResourceManagerComponentFactory(
                    configuration);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 JobManager 的三大核心角色实例
             *  1、webMonitorEndpoint：用于接收客户端发送的执行任务的rest请求
             *  2、resourceManager：负责资源的分配和记帐
             *  3、dispatcher：负责用于接收作业提交，持久化它们，生成要执行的作业管理器任务，并在主任务失败时恢复它们。
             */
            clusterComponent = dispatcherResourceManagerComponentFactory
                    .create(configuration, ioExecutor, commonRpcService, haServices, blobServer, heartbeatServices, metricRegistry, archivedExecutionGraphStore,
                            new RpcMetricQueryServiceRetriever(metricRegistry.getMetricQueryServiceRpcService()), this);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 如果创建失败，则关闭
             */
            clusterComponent.getShutDownFuture().whenComplete((ApplicationStatus applicationStatus, Throwable throwable) -> {
                if(throwable != null) {
                    shutDownAsync(ApplicationStatus.UNKNOWN, ExceptionUtils.stringifyException(throwable), false);
                } else {
                    // This is the general shutdown path. If a separate more
                    // specific shutdown was
                    // already triggered, this will do nothing
                    shutDownAsync(applicationStatus, null, true);
                }
            });
        }
    }

    protected void initializeServices(Configuration configuration, PluginManager pluginManager) throws Exception {

        LOG.info("Initializing cluster services.");

        synchronized(lock) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建一个 Akka rpc 服务 commonRpcService： 基于 Akka 的 RpcService 实现
             *  commonRpcService 其实是一个基于 akka 得 actorSystem，其实就是一个 tcp 的 rpc 服务，端口为：6123
             */
            commonRpcService = AkkaRpcServiceUtils
                    .createRemoteRpcService(configuration, configuration.getString(JobManagerOptions.ADDRESS), getRPCPortRange(configuration),
                            configuration.getString(JobManagerOptions.BIND_HOST), configuration.getOptional(JobManagerOptions.RPC_BIND_PORT));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动一个 JMXService
             *  Flink-1.12 新增加的一个功能
             */
            JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));

            // update the configuration used to create the high availability services
            configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动一个线程池
             *  如果你当前节点有 32 个 cpu ,那么当前这个 ioExecutor 启动的线程的数量为：128
             *  因为整个Flink 集群很多的地方的代码都是异步编程
             */
            ioExecutor = Executors.newFixedThreadPool(ClusterEntrypointUtils.getPoolSize(configuration), new ExecutorThreadFactory("cluster-io"));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 初始化 HA 高可用服务
             *  一般都搭建 基于 zk 的 HA 服务： ZooKeeperHaServices
             */
            haServices = createHaServices(configuration, ioExecutor);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 初始化 BlobServer
             *  主要管理一些大文件的上传等，比如用户作业的 jar 包、TaskManager 上传 log 文件等
             *  BlobService（C/S架构： BlobServer（BIO服务端）  BlobClient（BIO客户端））
             */
            blobServer = new BlobServer(configuration, haServices.createBlobStore());
            blobServer.start();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 初始化心跳服务，具体实现是：HeartBeatImpl
             *  后续的运行的一些心跳服务，都是基于这个 基础心跳服务来构建的
             */
            heartbeatServices = createHeartbeatServices(configuration);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： metrics（性能监控） 相关的服务
             *  有一些小伙伴的公司，可能需要去自研一些关于监控的一些平台
             */
            metricRegistry = createMetricRegistry(configuration, pluginManager);

            final RpcService metricQueryServiceRpcService = MetricUtils.startRemoteMetricsRpcService(configuration, commonRpcService.getAddress());
            metricRegistry.startQueryService(metricQueryServiceRpcService, null);

            final String hostname = RpcUtils.getHostname(commonRpcService);

            processMetricGroup = MetricUtils
                    .instantiateProcessMetricGroup(metricRegistry, hostname, ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： ArchivedExecutionGraphStore: 存储 ExecutionGraph 的服务， 默认有两种实现
             *  1、MemoryArchivedExecutionGraphStore 主要是在内存中缓存
             *  2、FileArchivedExecutionGraphStore 会持久化到文件系统，也会在内存中缓存
             *  默认实现是基于 File 的： FileArchivedExecutionGraphStore
             *  -
             *  1、per-job 模式在内存中
             *  2、session 模式在磁盘中
             *  -
             *  StreamGraph  JobGraph  ExeuctionGraph  物理执行图
             */
            archivedExecutionGraphStore = createSerializableExecutionGraphStore(configuration, commonRpcService.getScheduledExecutor());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 到时候，在初始化启动 Dispatcher 的时候，还会启动一个 JobGraphStore
             */

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： Flink Graph 有四层：
             *  1、StreamGraph
             *  2、JobGraph
             *  前面这两个东西，都是客户端创建的，所以其实，client提交job给sserver，提交的就是 JobGraph
             *  3、ExcutionGraph
             *  4、物理执行图
             *  后面这两个东西，都是在服务端创建的。
             */
        }
    }

    /**
     * Returns the port range for the common {@link RpcService}.
     *
     * @param configuration to extract the port range from
     * @return Port range for the common {@link RpcService}
     */
    protected String getRPCPortRange(Configuration configuration) {
        if(ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
            return configuration.getString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE);
        } else {
            return String.valueOf(configuration.getInteger(JobManagerOptions.PORT));
        }
    }

    protected HighAvailabilityServices createHaServices(Configuration configuration, Executor executor) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return HighAvailabilityServicesUtils
                .createHighAvailabilityServices(configuration, executor, HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);
    }

    protected HeartbeatServices createHeartbeatServices(Configuration configuration) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return HeartbeatServices.fromConfiguration(configuration);
    }

    protected MetricRegistryImpl createMetricRegistry(Configuration configuration, PluginManager pluginManager) {
        return new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration),
                ReporterSetup.fromConfiguration(configuration, pluginManager));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return shutDownAsync(ApplicationStatus.UNKNOWN, "Cluster entrypoint has been closed externally.", true).thenAccept(ignored -> {
        });
    }

    protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
        final long shutdownTimeout = configuration.getLong(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT);

        synchronized(lock) {
            Throwable exception = null;

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if(blobServer != null) {
                try {
                    blobServer.close();
                } catch(Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if(haServices != null) {
                try {
                    if(cleanupHaData) {
                        haServices.closeAndCleanupAllData();
                    } else {
                        haServices.close();
                    }
                } catch(Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if(archivedExecutionGraphStore != null) {
                try {
                    archivedExecutionGraphStore.close();
                } catch(Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if(processMetricGroup != null) {
                processMetricGroup.close();
            }

            if(metricRegistry != null) {
                terminationFutures.add(metricRegistry.shutdown());
            }

            if(ioExecutor != null) {
                terminationFutures.add(ExecutorUtils.nonBlockingShutdown(shutdownTimeout, TimeUnit.MILLISECONDS, ioExecutor));
            }

            if(commonRpcService != null) {
                terminationFutures.add(commonRpcService.stopService());
            }

            try {
                JMXService.stopInstance();
            } catch(Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            if(exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    @Override
    public void onFatalError(Throwable exception) {
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(exception);
        LOG.error("Fatal error occurred in the cluster entrypoint.", exception);

        System.exit(RUNTIME_FAILURE_RETURN_CODE);
    }

    // --------------------------------------------------
    // Internal methods
    // --------------------------------------------------

    private Configuration generateClusterConfiguration(Configuration configuration) {
        final Configuration resultConfiguration = new Configuration(Preconditions.checkNotNull(configuration));

        final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);
        final File uniqueWebTmpDir = new File(webTmpDir, "flink-web-" + UUID.randomUUID());

        resultConfiguration.setString(WebOptions.TMP_DIR, uniqueWebTmpDir.getAbsolutePath());

        return resultConfiguration;
    }

    private CompletableFuture<ApplicationStatus> shutDownAsync(ApplicationStatus applicationStatus, @Nullable String diagnostics, boolean cleanupHaData) {
        if(isShutDown.compareAndSet(false, true)) {
            LOG.info("Shutting {} down with application status {}. Diagnostics {}.", getClass().getSimpleName(), applicationStatus, diagnostics);

            final CompletableFuture<Void> shutDownApplicationFuture = closeClusterComponent(applicationStatus, diagnostics);

            final CompletableFuture<Void> serviceShutdownFuture = FutureUtils
                    .composeAfterwards(shutDownApplicationFuture, () -> stopClusterServices(cleanupHaData));

            final CompletableFuture<Void> cleanupDirectoriesFuture = FutureUtils.runAfterwards(serviceShutdownFuture, this::cleanupDirectories);

            cleanupDirectoriesFuture.whenComplete((Void ignored2, Throwable serviceThrowable) -> {
                if(serviceThrowable != null) {
                    terminationFuture.completeExceptionally(serviceThrowable);
                } else {
                    terminationFuture.complete(applicationStatus);
                }
            });
        }

        return terminationFuture;
    }

    /**
     * Deregister the Flink application from the resource management system by signalling the {@link
     * ResourceManager}.
     *
     * @param applicationStatus to terminate the application with
     * @param diagnostics       additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    private CompletableFuture<Void> closeClusterComponent(ApplicationStatus applicationStatus, @Nullable String diagnostics) {
        synchronized(lock) {
            if(clusterComponent != null) {
                return clusterComponent.deregisterApplicationAndClose(applicationStatus, diagnostics);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    /**
     * Clean up of temporary directories created by the {@link ClusterEntrypoint}.
     *
     * @throws IOException if the temporary directories could not be cleaned up
     */
    private void cleanupDirectories() throws IOException {
        ShutdownHookUtil.removeShutdownHook(shutDownHook, getClass().getSimpleName(), LOG);

        final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);

        FileUtils.deleteDirectory(new File(webTmpDir));
    }

    // --------------------------------------------------
    // Abstract methods
    // --------------------------------------------------

    protected abstract DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(
            Configuration configuration) throws IOException;

    protected abstract ArchivedExecutionGraphStore createSerializableExecutionGraphStore(Configuration configuration,
            ScheduledExecutor scheduledExecutor) throws IOException;

    protected static EntrypointClusterConfiguration parseArguments(String[] args) throws FlinkParseException {
        final CommandLineParser<EntrypointClusterConfiguration> clusterConfigurationParser = new CommandLineParser<>(
                new EntrypointClusterConfigurationParserFactory());

        return clusterConfigurationParser.parse(args);
    }

    protected static Configuration loadConfiguration(EntrypointClusterConfiguration entrypointClusterConfiguration) {

        final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(entrypointClusterConfiguration.getDynamicProperties());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final Configuration configuration = GlobalConfiguration.loadConfiguration(entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

        final int restPort = entrypointClusterConfiguration.getRestPort();

        if(restPort >= 0) {
            configuration.setInteger(RestOptions.PORT, restPort);
        }

        final String hostname = entrypointClusterConfiguration.getHostname();

        if(hostname != null) {
            configuration.setString(JobManagerOptions.ADDRESS, hostname);
        }

        return configuration;
    }

    // --------------------------------------------------
    // Helper methods
    // --------------------------------------------------

    public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {

        // TODO_MA 注释： clusterEntrypointName = StandaloneSessionClusterEntrypoint
        final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 通过父类方法来启动
             */
            clusterEntrypoint.startCluster();

        } catch(ClusterEntrypointException e) {
            LOG.error(String.format("Could not start cluster entrypoint %s.", clusterEntrypointName), e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 这是一个 java8 的异步编程的代码处理
         */
        clusterEntrypoint.getTerminationFuture().whenComplete((applicationStatus, throwable) -> {
            final int returnCode;

            if(throwable != null) {
                returnCode = RUNTIME_FAILURE_RETURN_CODE;
            } else {
                returnCode = applicationStatus.processExitCode();
            }

            LOG.info("Terminating cluster entrypoint process {} with exit code {}.", clusterEntrypointName, returnCode, throwable);
            System.exit(returnCode);
        });
    }

    /**
     * Execution mode of the {@link MiniDispatcher}.
     */
    public enum ExecutionMode {
        /**
         * Waits until the job result has been served.
         */
        NORMAL,

        /**
         * Directly stops after the job has finished.
         */
        DETACHED
    }
}
