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

package org.apache.flink.yarn.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.executors.AbstractJobClusterExecutor;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * The {@link PipelineExecutor} to be used when executing a job in isolation. This executor will
 * start a cluster specifically for the job at hand and tear it down when the job is finished either
 * successfully or due to an error.
 */
@Internal
public class YarnJobClusterExecutor extends AbstractJobClusterExecutor<ApplicationId, YarnClusterClientFactory> {

    // TODO_MA 注释： yarn-per-job
    public static final String NAME = YarnDeploymentTarget.PER_JOB.getName();

    public YarnJobClusterExecutor() {
        super(new YarnClusterClientFactory());
    }
}
