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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link LeaderElectionDriver} implementation for Zookeeper. The leading JobManager is elected
 * using ZooKeeper. The current leader's address as well as its leader session ID is published via
 * ZooKeeper.
 */
public class ZooKeeperLeaderElectionDriver implements LeaderElectionDriver, LeaderLatchListener, NodeCacheListener, UnhandledErrorListener {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionDriver.class);

    /**
     * Client to the ZooKeeper quorum.
     */
    private final CuratorFramework client;

    /**
     * Curator recipe for leader election.
     * // TODO_MA 注释: curator 组件中的一个 具体
     */
    private final LeaderLatch leaderLatch;

    /**
     * Curator recipe to watch a given ZooKeeper node for changes.
     */
    private final NodeCache cache;

    /**
     * ZooKeeper path of the node which stores the current leader information.
     */
    private final String leaderPath;

    private final ConnectionStateListener listener = (client, newState) -> handleStateChange(newState);

    private final LeaderElectionEventHandler leaderElectionEventHandler;

    private final FatalErrorHandler fatalErrorHandler;

    private final String leaderContenderDescription;

    private volatile boolean running;

    /**
     * Creates a ZooKeeperLeaderElectionDriver object.
     *
     * @param client                     Client which is connected to the ZooKeeper quorum
     * @param latchPath                  ZooKeeper node path for the leader election latch
     * @param leaderPath                 ZooKeeper node path for the node which stores the current leader
     *                                   information
     * @param leaderElectionEventHandler Event handler for processing leader change events
     * @param fatalErrorHandler          Fatal error handler
     * @param leaderContenderDescription Leader contender description
     */
    public ZooKeeperLeaderElectionDriver(CuratorFramework client, String latchPath, String leaderPath, LeaderElectionEventHandler leaderElectionEventHandler,
            FatalErrorHandler fatalErrorHandler, String leaderContenderDescription) throws Exception {
        this.client = checkNotNull(client);
        this.leaderPath = checkNotNull(leaderPath);
        this.leaderElectionEventHandler = checkNotNull(leaderElectionEventHandler);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.leaderContenderDescription = checkNotNull(leaderContenderDescription);

        // TODO_MA 注释： 创建 LeaderLatch 对象
        // TODO_MA 注释： LeaderLatch 是 curator 这个技术的一个选举 API
        leaderLatch = new LeaderLatch(client, checkNotNull(latchPath));
        cache = new NodeCache(client, leaderPath);

        client.getUnhandledErrorListenable().addListener(this);

        running = true;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 进行选举
         *  -
         *  此时注册了一个 this = Listener 意义等同于 ZK 中的 Watcher 监听器
         *  1、如果选举成为leader，则回调用 this.isLeader()
         *  2、如果选举失败，则回调 this.notLeader()
         */
        leaderLatch.addListener(this);
        leaderLatch.start();  // 开启选举！

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 选举开始之后，会在不久接收到响应：
         *  1、如果选举成功，则回调该类的 isLeader() 方法。
         *  2、如果选举失败，则回调该类的 notLeader() 方法。
         */

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 这是监听！
         *  Cache 是 Curator 对事件监听的包装，会自动的再次监听，我们就不需要自己手动的重复监听了。
         *  补充： ZooKeeper 的 Cache 有三种：
         *  NodeCache：用来监听节点的数据变化，当监听的节点的数据发生变化的时候就会回调对应的函数
         *              回调：this.nodeChanged() 方法
         *  PathChildrenCache：用来监听指定节点 的子节点变化情况
         *              回调：this.childEvent() 方法
         *  TreeCache：TreeCache既能够监听自身节点的变化、也能够监听子节点的变化
         *              回调：this.childEvent() 方法
         */
        cache.getListenable().addListener(this);
        cache.start();

        client.getConnectionStateListenable().addListener(listener);
    }

    @Override
    public void close() throws Exception {
        if(!running) {
            return;
        }
        running = false;

        LOG.info("Closing {}", this);

        client.getUnhandledErrorListenable().removeListener(this);

        client.getConnectionStateListenable().removeListener(listener);

        Exception exception = null;

        try {
            cache.close();
        } catch(Exception e) {
            exception = e;
        }

        try {
            leaderLatch.close();
        } catch(Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if(exception != null) {
            throw new Exception("Could not properly stop the ZooKeeperLeaderElectionDriver.", exception);
        }
    }

    @Override
    public boolean hasLeadership() {
        assert (running);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return leaderLatch.hasLeadership();
    }

    @Override
    public void isLeader() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        leaderElectionEventHandler.onGrantLeadership(); //leaderElectionEventHandler = DefaultLeaderElectionService
    }

    @Override
    public void notLeader() {
        leaderElectionEventHandler.onRevokeLeadership();
    }

    @Override
    public void nodeChanged() throws Exception {
        if(leaderLatch.hasLeadership()) {

            // TODO_MA 注释： 获取新数据
            ChildData childData = cache.getCurrentData();

            // TODO_MA 注释： 处理数据
            if(childData != null) {
                final byte[] data = childData.getData();
                if(data != null && data.length > 0) {
                    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    final ObjectInputStream ois = new ObjectInputStream(bais);

                    final String leaderAddress = ois.readUTF();
                    final UUID leaderSessionID = (UUID) ois.readObject();

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 写入 Leader 信息到 ZK 中
                     */
                    leaderElectionEventHandler.onLeaderInformationChange(LeaderInformation.known(leaderSessionID, leaderAddress));
                    return;
                }
            }

            // TODO_MA 注释： 写入 Leader 信息到 ZK 中
            leaderElectionEventHandler.onLeaderInformationChange(LeaderInformation.empty());
        }
    }

    /**
     * Writes the current leader's address as well the given leader session ID to ZooKeeper.
     */
    @Override
    public void writeLeaderInformation(LeaderInformation leaderInformation) {
        assert (running);
        // this method does not have to be synchronized because the curator framework client
        // is thread-safe. We do not write the empty data to ZooKeeper here. Because
        // check-leadership-and-update
        // is not a transactional operation. We may wrongly clear the data written by new leader.
        if(LOG.isDebugEnabled()) {
            LOG.debug("Write leader information: {}.", leaderInformation);
        }
        if(leaderInformation.isEmpty()) {
            return;
        }

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeUTF(leaderInformation.getLeaderAddress());
            oos.writeObject(leaderInformation.getLeaderSessionID());

            oos.close();

            boolean dataWritten = false;

            while(!dataWritten && leaderLatch.hasLeadership()) {

                // TODO_MA 注释： 检查 znode 节点是否存在
                Stat stat = client.checkExists().forPath(leaderPath);

                if(stat != null) {
                    long owner = stat.getEphemeralOwner();
                    long sessionID = client.getZookeeperClient().getZooKeeper().getSessionId();

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 写入 zk 中
                     */
                    if(owner == sessionID) {
                        try {

                            // TODO_MA 注释： 写入数据
                            client.setData().forPath(leaderPath, baos.toByteArray());

                            dataWritten = true;
                        } catch(KeeperException.NoNodeException noNode) {
                            // node was deleted in the meantime
                        }
                    } else {
                        try {
                            client.delete().forPath(leaderPath);
                        } catch(KeeperException.NoNodeException noNode) {
                            // node was deleted in the meantime --> try again
                        }
                    }
                } else {
                    try {
                        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(leaderPath, baos.toByteArray());
                        dataWritten = true;
                    } catch(KeeperException.NodeExistsException nodeExists) {
                        // node has been created in the meantime --> try again
                    }
                }
            }

            if(LOG.isDebugEnabled()) {
                LOG.debug("Successfully wrote leader information: {}.", leaderInformation);
            }
        } catch(Exception e) {
            fatalErrorHandler.onFatalError(new LeaderElectionException("Could not write leader address and leader session ID to " + "ZooKeeper.", e));
        }
    }

    private void handleStateChange(ConnectionState newState) {
        switch(newState) {
            case CONNECTED:
                LOG.debug("Connected to ZooKeeper quorum. Leader election can start.");
                break;
            case SUSPENDED:
                LOG.warn("Connection to ZooKeeper suspended. The contender " + leaderContenderDescription + " no longer participates in the leader election.");
                break;
            case RECONNECTED:
                LOG.info("Connection to ZooKeeper was reconnected. Leader election can be restarted.");
                break;
            case LOST:
                // Maybe we have to throw an exception here to terminate the JobManager
                LOG.warn("Connection to ZooKeeper lost. The contender " + leaderContenderDescription + " no longer participates in the leader election.");
                break;
        }
    }

    @Override
    public void unhandledError(String message, Throwable e) {
        fatalErrorHandler.onFatalError(new LeaderElectionException("Unhandled error in ZooKeeperLeaderElectionDriver: " + message, e));
    }

    @Override
    public String toString() {
        return "ZooKeeperLeaderElectionDriver{" + "leaderPath='" + leaderPath + '\'' + '}';
    }
}
