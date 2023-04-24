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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConnectionManager implements ConnectionManager {

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    private final NettyServer server;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    private final NettyClient client;

    private final NettyBufferPool bufferPool;

    private final PartitionRequestClientFactory partitionRequestClientFactory;

    private final NettyProtocol nettyProtocol;

    public NettyConnectionManager(ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 NettyServer 和 NettyClient
         *  在初始化之后，要绑定一些 Handler 来执行相应的请求的处理
         */
        this.server = new NettyServer(nettyConfig);
        this.client = new NettyClient(nettyConfig);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化得到 BufferPool
         */
        this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        this.partitionRequestClientFactory = new PartitionRequestClientFactory(client, nettyConfig.getNetworkRetries());

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化得到一个 NettyProtocol， 用于获取 Handler
         *  1、Server 端的 Handler　有两个：
         *      PartitionRequestQueue
         *      PartitionRequestServerHandler
         *  2、Client 端的 Handler 有一个：
         *      CreditBasedPartitionRequestClientHandler
         */
        this.nettyProtocol = new NettyProtocol(checkNotNull(partitionProvider), checkNotNull(taskEventPublisher));
    }

    @Override
    public int start() throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： NettyClient 启动
         */
        client.init(nettyProtocol, bufferPool);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： NettyServer 启动
         */
        return server.init(nettyProtocol, bufferPool);
    }

    @Override
    public PartitionRequestClient createPartitionRequestClient(
            ConnectionID connectionId) throws IOException, InterruptedException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
    }

    @Override
    public void closeOpenChannelConnections(ConnectionID connectionId) {
        partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
    }

    @Override
    public int getNumberOfActiveConnections() {
        return partitionRequestClientFactory.getNumberOfActiveClients();
    }

    @Override
    public void shutdown() {
        client.shutdown();
        server.shutdown();
    }

    NettyClient getClient() {
        return client;
    }

    NettyServer getServer() {
        return server;
    }

    NettyBufferPool getBufferPool() {
        return bufferPool;
    }
}
