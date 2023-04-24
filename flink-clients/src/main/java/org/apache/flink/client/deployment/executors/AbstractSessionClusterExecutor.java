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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on an existing
 * (session) cluster.
 *
 * @param <ClusterID>     the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a
 *                        client to the target cluster.
 */
@Internal
public class AbstractSessionClusterExecutor<ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>> implements PipelineExecutor {

    private final ClientFactory clusterClientFactory;

    public AbstractSessionClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： Pipeline = 其实就是 StreamGraph
     */
    @Override
    public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration,
            @Nonnull final ClassLoader userCodeClassloader) throws Exception {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取 JobGraph
         *  pipeline 其实就是 StreamGraph
         */
        final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         *  1、到此为止，要提交的内容已经有： JobGraph
         *  2、但是，通过什么来提交，怎么提交还不确定！
         */

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： clusterDescriptor = StandaloneClusterDescriptor
         */
        try(final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 用于创建 RestClusterClient 的Provider: ClusterClientProvider
             *  1、其实内部就是会初始化得到： RestClusterClient
             *  2、在初始化 RestClusterClient 的时候，也会初始化他内部的成员变量： RestClient
             *  3、在初始化 RestClient 的时候，也会初始化他内部的一个 netty 客户端
             *  提交 Job 的客户端： netty客户端 ==> RestClient ==> RestClusterClient
             *  接收 job 的服务端： JobManager 中启动的 WebmonitorEndpoint 内部所启动的 Netty 服务端
             */
            final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor.retrieve(clusterID);

            // TODO_MA 注释： clusterClient = RestClusterClient
            ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 提交执行
             *  1、MiniClusterClient 本地执行
             *  2、RestClusterClient 提交到 Flink Rest 服务接收处理
             *  -
             *  一定是调用： RestClient 进行提交
             *  一定是通过 RestClient 内部的 netty 客户端进行提交
             */
            return clusterClient.submitJob(jobGraph).thenApplyAsync(FunctionUtils.uncheckedFunction(jobId -> {
                ClientUtils.waitUntilJobInitializationFinished(() -> clusterClient.getJobStatus(jobId).get(), () -> clusterClient.requestJobResult(jobId).get(),
                        userCodeClassloader);
                return jobId;
            })).thenApplyAsync(jobID -> (JobClient) new ClusterClientJobClientAdapter<>(clusterClientProvider, jobID, userCodeClassloader))
                    .whenComplete((ignored1, ignored2) -> clusterClient.close());

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 其实在这个方法的内部，就是调用 RestClusterClient 这个变量的成员变量： RestClient 来执行提交
             */
        }
    }
}
