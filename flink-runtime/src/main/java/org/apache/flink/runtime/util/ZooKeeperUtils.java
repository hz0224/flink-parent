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

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultLastStateConnectionStateListener;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointStoreUtil;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreUtil;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreWatcher;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.ZooKeeperLeaderElectionDriverFactory;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriverFactory;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriverFactory;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.flink.shaded.curator4.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.ZooDefs;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class containing helper functions to interact with ZooKeeper.
 */
public class ZooKeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

    /**
     * The prefix of the submitted job graph file.
     */
    public static final String HA_STORAGE_SUBMITTED_JOBGRAPH_PREFIX = "submittedJobGraph";

    /**
     * The prefix of the completed checkpoint file.
     */
    public static final String HA_STORAGE_COMPLETED_CHECKPOINT = "completedCheckpoint";

    /**
     * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper quorum.
     *
     * @param configuration {@link Configuration} object containing the configuration values
     * @return {@link CuratorFramework} instance
     */
    public static CuratorFramework startCuratorFramework(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "configuration");

        String zkQuorum = configuration.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);

        if(zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new RuntimeException(
                    "No valid ZooKeeper quorum has been specified. " + "You can specify the quorum via the configuration key '" + HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM
                            .key() + "'.");
        }

        int sessionTimeout = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT);

        int connectionTimeout = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT);

        int retryWait = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_RETRY_WAIT);

        int maxRetryAttempts = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS);

        String root = configuration.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT);

        String namespace = configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);

        boolean disableSaslClient = configuration.getBoolean(SecurityOptions.ZOOKEEPER_SASL_DISABLE);

        ACLProvider aclProvider;

        ZkClientACLMode aclMode = ZkClientACLMode.fromConfig(configuration);

        if(disableSaslClient && aclMode == ZkClientACLMode.CREATOR) {
            String errorMessage = "Cannot set ACL role to " + aclMode + "  since SASL authentication is " + "disabled through the " + SecurityOptions.ZOOKEEPER_SASL_DISABLE
                    .key() + " property";
            LOG.warn(errorMessage);
            throw new IllegalConfigurationException(errorMessage);
        }

        if(aclMode == ZkClientACLMode.CREATOR) {
            LOG.info("Enforcing creator for ZK connections");
            aclProvider = new SecureAclProvider();
        } else {
            LOG.info("Enforcing default ACL for ZK connections");
            aclProvider = new DefaultACLProvider();
        }

        String rootWithNamespace = generateZookeeperPath(root, namespace);

        LOG.info("Using '{}' as Zookeeper namespace.", rootWithNamespace);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 内部包装了  ZK 实例
         */
        CuratorFramework cf = CuratorFrameworkFactory.builder().connectString(zkQuorum).sessionTimeoutMs(sessionTimeout)
                .connectionTimeoutMs(connectionTimeout)
                .retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
                // Curator prepends a '/' manually and throws an Exception if the
                // namespace starts with a '/'.
                .namespace(rootWithNamespace.startsWith("/") ? rootWithNamespace.substring(1) : rootWithNamespace)
                .aclProvider(aclProvider).build();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        cf.start();

        return cf;
    }

    /**
     * Returns whether {@link HighAvailabilityMode#ZOOKEEPER} is configured.
     */
    public static boolean isZooKeeperRecoveryMode(Configuration flinkConf) {
        return HighAvailabilityMode.fromConfig(flinkConf).equals(HighAvailabilityMode.ZOOKEEPER);
    }

    /**
     * Returns the configured ZooKeeper quorum (and removes whitespace, because ZooKeeper does not
     * tolerate it).
     */
    public static String getZooKeeperEnsemble(Configuration flinkConf) throws IllegalConfigurationException {

        String zkQuorum = flinkConf.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);

        if(zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new IllegalConfigurationException("No ZooKeeper quorum specified in config.");
        }

        // Remove all whitespace
        zkQuorum = zkQuorum.replaceAll("\\s+", "");

        return zkQuorum;
    }

    /**
     * Creates a {@link DefaultLeaderRetrievalService} instance with {@link
     * ZooKeeperLeaderRetrievalDriver}.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @return {@link DefaultLeaderRetrievalService} instance.
     */
    public static DefaultLeaderRetrievalService createLeaderRetrievalService(final CuratorFramework client,
            final Configuration configuration) {
        return createLeaderRetrievalService(client, configuration, "");
    }

    /**
     * Creates a {@link DefaultLeaderRetrievalService} instance with {@link
     * ZooKeeperLeaderRetrievalDriver}.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param pathSuffix    The path suffix which we want to append
     * @return {@link DefaultLeaderRetrievalService} instance.
     */
    public static DefaultLeaderRetrievalService createLeaderRetrievalService(final CuratorFramework client,
            final Configuration configuration, final String pathSuffix) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new DefaultLeaderRetrievalService(createLeaderRetrievalDriverFactory(client, configuration, pathSuffix));
    }

    /**
     * Creates a {@link LeaderRetrievalDriverFactory} implemented by ZooKeeper.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @return {@link LeaderRetrievalDriverFactory} instance.
     */
    public static ZooKeeperLeaderRetrievalDriverFactory createLeaderRetrievalDriverFactory(final CuratorFramework client,
            final Configuration configuration) {
        return createLeaderRetrievalDriverFactory(client, configuration, "");
    }

    /**
     * Creates a {@link LeaderRetrievalDriverFactory} implemented by ZooKeeper.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param pathSuffix    The path suffix which we want to append
     * @return {@link LeaderRetrievalDriverFactory} instance.
     */
    public static ZooKeeperLeaderRetrievalDriverFactory createLeaderRetrievalDriverFactory(final CuratorFramework client,
            final Configuration configuration, final String pathSuffix) {
        final String leaderPath = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_LEADER_PATH) + pathSuffix;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new ZooKeeperLeaderRetrievalDriverFactory(client, leaderPath);
    }

    /**
     * Creates a {@link DefaultLeaderElectionService} instance with {@link
     * ZooKeeperLeaderElectionDriver}.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @return {@link DefaultLeaderElectionService} instance.
     */
    public static DefaultLeaderElectionService createLeaderElectionService(CuratorFramework client,
            Configuration configuration) {

        return createLeaderElectionService(client, configuration, "");
    }

    /**
     * Creates a {@link DefaultLeaderElectionService} instance with {@link
     * ZooKeeperLeaderElectionDriver}.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param pathSuffix    The path suffix which we want to append
     * @return {@link DefaultLeaderElectionService} instance.
     */
    public static DefaultLeaderElectionService createLeaderElectionService(final CuratorFramework client,
            final Configuration configuration, final String pathSuffix) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new DefaultLeaderElectionService(createLeaderElectionDriverFactory(client, configuration, pathSuffix));
    }

    /**
     * Creates a {@link LeaderElectionDriverFactory} implemented by ZooKeeper.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @return {@link LeaderElectionDriverFactory} instance.
     */
    public static ZooKeeperLeaderElectionDriverFactory createLeaderElectionDriverFactory(final CuratorFramework client,
            final Configuration configuration) {
        return createLeaderElectionDriverFactory(client, configuration, "");
    }

    /**
     * Creates a {@link LeaderElectionDriverFactory} implemented by ZooKeeper.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param pathSuffix    The path suffix which we want to append
     * @return {@link LeaderElectionDriverFactory} instance.
     */
    public static ZooKeeperLeaderElectionDriverFactory createLeaderElectionDriverFactory(final CuratorFramework client,
            final Configuration configuration, final String pathSuffix) {
        final String latchPath = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_LATCH_PATH) + pathSuffix;
        final String leaderPath = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_LEADER_PATH) + pathSuffix;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建一个基于 ZooKeeper 的 LeaderElectionDriver 工厂：ZooKeeperLeaderElectionDriverFactory
         */
        return new ZooKeeperLeaderElectionDriverFactory(client, latchPath, leaderPath);
    }

    /**
     * Creates a {@link DefaultJobGraphStore} instance with {@link ZooKeeperStateHandleStore},
     * {@link ZooKeeperJobGraphStoreWatcher} and {@link ZooKeeperJobGraphStoreUtil}.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object
     * @return {@link DefaultJobGraphStore} instance
     * @throws Exception if the submitted job graph store cannot be created
     */
    public static JobGraphStore createJobGraphs(CuratorFramework client, Configuration configuration) throws Exception {

        checkNotNull(configuration, "Configuration");

        // TODO_MA 注释： 创建 FileSystemStateStorage
        RetrievableStateStorageHelper<JobGraph> stateStorage = createFileSystemStateStorage(configuration,
                HA_STORAGE_SUBMITTED_JOBGRAPH_PREFIX);

        // ZooKeeper submitted jobs root dir
        String zooKeeperJobsPath = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_JOBGRAPHS_PATH);

        // Ensure that the job graphs path exists
        client.newNamespaceAwareEnsurePath(zooKeeperJobsPath).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        CuratorFramework facade = client.usingNamespace(client.getNamespace() + zooKeeperJobsPath);

        final String zooKeeperFullJobsPath = client.getNamespace() + zooKeeperJobsPath;

        // TODO_MA 注释： 创建 基于 ZK 的 StateHandleStore： ZooKeeperStateHandleStore
        final ZooKeeperStateHandleStore<JobGraph> zooKeeperStateHandleStore = new ZooKeeperStateHandleStore<>(facade,
                stateStorage);

        // TODO_MA 注释： 监听孩子节点列表变化的 ZK 监听器
        final PathChildrenCache pathCache = new PathChildrenCache(facade, "/", false);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 基于 ZK 的 JobGraphStore
         */
        return new DefaultJobGraphStore<>(zooKeeperStateHandleStore, new ZooKeeperJobGraphStoreWatcher(pathCache),
                ZooKeeperJobGraphStoreUtil.INSTANCE);
    }

    /**
     * Creates a {@link DefaultCompletedCheckpointStore} instance with {@link
     * ZooKeeperStateHandleStore}.
     *
     * @param client                         The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration                  {@link Configuration} object
     * @param jobId                          ID of job to create the instance for
     * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain
     * @param executor                       to run ZooKeeper callbacks
     * @return {@link DefaultCompletedCheckpointStore} instance
     * @throws Exception if the completed checkpoint store cannot be created
     */
    public static CompletedCheckpointStore createCompletedCheckpoints(CuratorFramework client,
            Configuration configuration, JobID jobId, int maxNumberOfCheckpointsToRetain,
            Executor executor) throws Exception {

        checkNotNull(configuration, "Configuration");

        String checkpointsPath = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_CHECKPOINTS_PATH);

        RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage = createFileSystemStateStorage(configuration,
                HA_STORAGE_COMPLETED_CHECKPOINT);

        checkpointsPath += getPathForJob(jobId);

        // TODO_MA 注释： ZooKeeperStateHandleStore
        final ZooKeeperStateHandleStore<CompletedCheckpoint> completedCheckpointStateHandleStore = createZooKeeperStateHandleStore(
                client, checkpointsPath, stateStorage);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        final CompletedCheckpointStore zooKeeperCompletedCheckpointStore = new DefaultCompletedCheckpointStore<>(
                maxNumberOfCheckpointsToRetain, completedCheckpointStateHandleStore,
                ZooKeeperCheckpointStoreUtil.INSTANCE, executor);

        LOG.info("Initialized {} in '{}' with {}.", DefaultCompletedCheckpointStore.class.getSimpleName(),
                completedCheckpointStateHandleStore, checkpointsPath);
        return zooKeeperCompletedCheckpointStore;
    }

    /**
     * Returns the JobID as a String (with leading slash).
     */
    public static String getPathForJob(JobID jobId) {
        checkNotNull(jobId, "Job ID");
        return String.format("/%s", jobId);
    }

    /**
     * Creates an instance of {@link ZooKeeperStateHandleStore}.
     *
     * @param client       ZK client
     * @param path         Path to use for the client namespace
     * @param stateStorage RetrievableStateStorageHelper that persist the actual state and whose
     *                     returned state handle is then written to ZooKeeper
     * @param <T>          Type of state
     * @return {@link ZooKeeperStateHandleStore} instance
     * @throws Exception ZK errors
     */
    public static <T extends Serializable> ZooKeeperStateHandleStore<T> createZooKeeperStateHandleStore(
            final CuratorFramework client, final String path,
            final RetrievableStateStorageHelper<T> stateStorage) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new ZooKeeperStateHandleStore<>(useNamespaceAndEnsurePath(client, path), stateStorage);
    }

    /**
     * Creates a {@link ZooKeeperCheckpointIDCounter} instance.
     *
     * @param client        The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object
     * @param jobId         ID of job to create the instance for
     * @return {@link ZooKeeperCheckpointIDCounter} instance
     */
    public static ZooKeeperCheckpointIDCounter createCheckpointIDCounter(CuratorFramework client,
            Configuration configuration, JobID jobId) {

        String checkpointIdCounterPath = configuration
                .getString(HighAvailabilityOptions.HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH);

        checkpointIdCounterPath += getPathForJob(jobId);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、注意监听器： DefaultLastStateConnectionStateListener
         */
        return new ZooKeeperCheckpointIDCounter(client, checkpointIdCounterPath,
                new DefaultLastStateConnectionStateListener());
    }

    /**
     * Creates a {@link FileSystemStateStorageHelper} instance.
     *
     * @param configuration {@link Configuration} object
     * @param prefix        Prefix for the created files
     * @param <T>           Type of the state objects
     * @return {@link FileSystemStateStorageHelper} instance
     * @throws IOException if file system state storage cannot be created
     */
    public static <T extends Serializable> FileSystemStateStorageHelper<T> createFileSystemStateStorage(
            Configuration configuration, String prefix) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return new FileSystemStateStorageHelper<>(
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration), prefix);
    }

    public static String generateZookeeperPath(String root, String namespace) {
        if(!namespace.startsWith("/")) {
            namespace = '/' + namespace;
        }

        if(namespace.endsWith("/")) {
            namespace = namespace.substring(0, namespace.length() - 1);
        }

        if(root.endsWith("/")) {
            root = root.substring(0, root.length() - 1);
        }

        return root + namespace;
    }

    /**
     * Returns a facade of the client that uses the specified namespace, and ensures that all nodes
     * in the path exist.
     *
     * @param client ZK client
     * @param path   the new namespace
     * @return ZK Client that uses the new namespace
     * @throws Exception ZK errors
     */
    public static CuratorFramework useNamespaceAndEnsurePath(final CuratorFramework client,
            final String path) throws Exception {
        Preconditions.checkNotNull(client, "client must not be null");
        Preconditions.checkNotNull(path, "path must not be null");

        // Ensure that the checkpoints path exists
        client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        return client.usingNamespace(generateZookeeperPath(client.getNamespace(), path));
    }

    /**
     * Secure {@link ACLProvider} implementation.
     */
    public static class SecureAclProvider implements ACLProvider {
        @Override
        public List<ACL> getDefaultAcl() {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }

        @Override
        public List<ACL> getAclForPath(String path) {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }
    }

    /**
     * ZooKeeper client ACL mode enum.
     */
    public enum ZkClientACLMode {
        CREATOR, OPEN;

        /**
         * Return the configured {@link ZkClientACLMode}.
         *
         * @param config The config to parse
         * @return Configured ACL mode or the default defined by {@link
         * HighAvailabilityOptions#ZOOKEEPER_CLIENT_ACL} if not configured.
         */
        public static ZkClientACLMode fromConfig(Configuration config) {
            String aclMode = config.getString(HighAvailabilityOptions.ZOOKEEPER_CLIENT_ACL);
            if(aclMode == null || aclMode.equalsIgnoreCase(ZkClientACLMode.OPEN.name())) {
                return ZkClientACLMode.OPEN;
            } else if(aclMode.equalsIgnoreCase(ZkClientACLMode.CREATOR.name())) {
                return ZkClientACLMode.CREATOR;
            } else {
                String message = "Unsupported ACL option: [" + aclMode + "] provided";
                LOG.error(message);
                throw new IllegalConfigurationException(message);
            }
        }
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private ZooKeeperUtils() {
        throw new RuntimeException();
    }
}
