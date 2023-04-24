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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

/**
 * Entry point for the standalone session cluster.
 */
public class StandaloneSessionClusterEntrypoint extends SessionClusterEntrypoint {

    public StandaloneSessionClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 第一个工厂： StandaloneResourceManagerFactory
         */
        return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                StandaloneResourceManagerFactory.getInstance()
        );
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 启动入口
     *  Flink 特别难看：
     *  1、名称都特别长
     *  2、方法参数都特别多
     */
    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneSessionClusterEntrypoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        // TODO_MA 注释： 解析 main 方法参数， 命令参数  flink run ....
        // TODO_MA 注释： 类似于 HDFS 的 Configuration
        final EntrypointClusterConfiguration entrypointClusterConfiguration = ClusterEntrypointUtils
                .parseParametersOrExit(args, new EntrypointClusterConfigurationParserFactory(), StandaloneSessionClusterEntrypoint.class);

        // TODO_MA 注释： 解析 flink-conf.yaml 配置文件
        Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 构建一个 Flink Standalone 集群的主节点启动实例
         *  1、Flink Standalone 模式： StandaloneSessionClusterEntrypoint
         *  2、Flink on YARN Per-job 模式： JobClusterEntrypoint
         *  3、Flink on YARN Session 模式： YarnSessionClusterEntrypoint
         */
        StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动
         */
        ClusterEntrypoint.runClusterEntrypoint(entrypoint);
    }
}
