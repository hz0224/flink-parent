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

package org.apache.flink.yarn.cli;

import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.executors.YarnJobClusterExecutor;
import org.apache.flink.yarn.executors.YarnSessionClusterExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

abstract class AbstractYarnCli extends AbstractCustomCommandLine {

    public static final String ID = "yarn-cluster";

    protected final Option applicationId;

    // TODO_MA 注释： -m 指定 jobManager 的地址
    protected final Option addressOption =
            new Option("m", "jobmanager", true, "Set to " + ID + " to use YARN execution mode.");

    protected final Configuration configuration;

    protected AbstractYarnCli(Configuration configuration, String shortPrefix, String longPrefix) {
        this.configuration = configuration;
        this.applicationId =
                new Option(
                        shortPrefix + "id",
                        longPrefix + "applicationId",
                        true,
                        "Attach to running YARN session");
    }

    @Override
    public boolean isActive(CommandLine commandLine) {

        // TODO_MA 注释： 获取 -m 指定的参数值
        final String jobManagerOption = commandLine.getOptionValue(addressOption.getOpt(), null);

        // TODO_MA 注释： ID 是固定字符串： yarn-cluster
        // TODO_MA 注释： flink-1.10 以前的用法： flink -m yarn-cluster -c XXX xxx.jar
        // TODO_MA 注释： flink-1.11 以后的用法： flink -t yarn-per-job -c XXX xxx.jar
        final boolean yarnJobManager = ID.equals(jobManagerOption);

        // TODO_MA 注释： 获取 yarn app ID
        final boolean hasYarnAppId =
                commandLine.hasOption(applicationId.getOpt())
                        // TODO_MA 注释： yarn.application.id
                        || configuration.getOptional(YarnConfigOptions.APPLICATION_ID).isPresent();

        // TODO_MA 注释：
        final boolean hasYarnExecutor =
                // TODO_MA 注释： yarn-session
                YarnSessionClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET))
                // TODO_MA 注释： yarn-per-job
             || YarnJobClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 总结一下： 三个条件，满足其一即可
         *  1、Executor 指定为 YARN
         *  2、-m 指定为 yarn-cluster
         *  3、yarn 有 AppID, 或者程序指定了
         */
        return hasYarnExecutor || yarnJobManager || hasYarnAppId;
    }

    @Override
    public void addGeneralOptions(Options baseOptions) {
        super.addGeneralOptions(baseOptions);
        baseOptions.addOption(applicationId);
        baseOptions.addOption(addressOption);
    }

    @Override
    public String getId() {
        return ID;
    }
}
