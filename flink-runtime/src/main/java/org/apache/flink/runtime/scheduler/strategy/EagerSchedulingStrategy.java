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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for streaming job which will schedule all tasks at the same
 * time.
 */
public class EagerSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    private final DeploymentOption deploymentOption = new DeploymentOption(false);

    public EagerSchedulingStrategy(SchedulerOperations schedulerOperations, SchedulingTopology schedulingTopology) {
        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);
    }

    @Override
    public void startScheduling() {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 重点： 两个步骤
         *  1、allocateSlots() 申请 slot
         *  2、deploy() 部署 StreamTask 运行
         *  -
         *  注意参数：
         *  Set<ExecutionVertexID> verticesToDeploy = SchedulingStrategyUtils.getAllVertexIdsFromTopology(schedulingTopology)
         *  -
         *  此时，JobMaster 已经把 JObGraph 转换成了 ExecutionGraph 的。
         *  所以此时调度的数据结构是： ExecutionGraph（中的每个顶点: ExecutionVertex）
         *  ExecutionGraph 在调度之前，已经被转换成了一个： Topology
         *  其实这个参数的提取，就是从 Topology 当中，获取到的所有的顶点的 ID 的集合
         *  -
         *  Topology 类似于 Spark 的 DAG
         *  关于 slot 的使用有一个共享的机制： 让同一个 job 中的不同的 Operator 的 Task 执行在一个 slot 中
         *  假设： 总 Task 数量 = m
         *        需要申请的 slot 的数量 = n
         *        关系： m >= n
         *  在有 slot 共享的情况下， verticesToDeploy.size()  <= m
         *  -
         *  在这个方法的内部：
         *  1、一定是需要做如下转换： Set<ExecutionVertexID> => Set<ExecutionVertex> => Set<Execution>
         *      -
         *      最后的 deploy 其实就是：  Execution.deploy()
         */
        allocateSlotsAndDeploy(SchedulingStrategyUtils.getAllVertexIdsFromTopology(schedulingTopology));
    }

    @Override
    public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
        allocateSlotsAndDeploy(verticesToRestart);
    }

    @Override
    public void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionState executionState) {
        // Will not react to these notifications.
    }

    @Override
    public void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId) {
        // Will not react to these notifications.
    }

    private void allocateSlotsAndDeploy(final Set<ExecutionVertexID> verticesToDeploy) {

        // TODO_MA 注释： 把每个 ExecutionVertexID 转换成一个 ExecutionVertexDeploymentOption 对象
        final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions = SchedulingStrategyUtils
                .createExecutionVertexDeploymentOptionsInTopologicalOrder(schedulingTopology, verticesToDeploy,
                        id -> deploymentOption);

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 为每个 ExecutionVertex 获取 DeploymentOption 信息
         */
        schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
    }

    /**
     * The factory for creating {@link EagerSchedulingStrategy}.
     */
    public static class Factory implements SchedulingStrategyFactory {

        @Override
        public SchedulingStrategy createInstance(SchedulerOperations schedulerOperations,
                SchedulingTopology schedulingTopology) {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            return new EagerSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
