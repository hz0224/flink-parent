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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class IntermediateResult {

    private final IntermediateDataSetID id;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 该 IntermediateResult 负责管理的哪个 ExecutionJobVertex 的输出
     */
    private final ExecutionJobVertex producer;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 一个 IntermediateResult 内部分成多个 IntermediateResultPartition
     */
    private final IntermediateResultPartition[] partitions;

    /**
     * Maps intermediate result partition IDs to a partition index. This is used for ID lookups of
     * intermediate results. I didn't dare to change the partition connect logic in other places
     * that is tightly coupled to the partitions being held as an array.
     */
    private final HashMap<IntermediateResultPartitionID, Integer> partitionLookupHelper = new HashMap<>();

    private final int numParallelProducers;

    private final AtomicInteger numberOfRunningProducers;

    private int partitionsAssigned;

    private int numConsumers;

    private final int connectionIndex;

    private final ResultPartitionType resultType;

    public IntermediateResult(IntermediateDataSetID id, ExecutionJobVertex producer, int numParallelProducers,
            ResultPartitionType resultType) {

        this.id = checkNotNull(id);
        this.producer = checkNotNull(producer);

        checkArgument(numParallelProducers >= 1);
        this.numParallelProducers = numParallelProducers;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化得到一个 IntermediateResultPartition 数组
         *  一个 ExecutionJobVertex 会根据并行度，生成多个 ExecutionVertex
         *  IntermediateResultPartition 就是负责给 ExecutionVertex 进行输出的
         */
        this.partitions = new IntermediateResultPartition[numParallelProducers];

        this.numberOfRunningProducers = new AtomicInteger(numParallelProducers);

        // we do not set the intermediate result partitions here, because we let them be initialized
        // by
        // the execution vertex that produces them

        // assign a random connection index
        this.connectionIndex = (int) (Math.random() * Integer.MAX_VALUE);

        // The runtime type for this produced result
        this.resultType = checkNotNull(resultType);
    }

    public void setPartition(int partitionNumber, IntermediateResultPartition partition) {
        if(partition == null || partitionNumber < 0 || partitionNumber >= numParallelProducers) {
            throw new IllegalArgumentException();
        }

        if(partitions[partitionNumber] != null) {
            throw new IllegalStateException("Partition #" + partitionNumber + " has already been assigned.");
        }

        partitions[partitionNumber] = partition;
        partitionLookupHelper.put(partition.getPartitionId(), partitionNumber);
        partitionsAssigned++;
    }

    public IntermediateDataSetID getId() {
        return id;
    }

    public ExecutionJobVertex getProducer() {
        return producer;
    }

    public IntermediateResultPartition[] getPartitions() {
        return partitions;
    }

    /**
     * Returns the partition with the given ID.
     *
     * @param resultPartitionId ID of the partition to look up
     * @return Intermediate result partition with the given ID
     * @throws NullPointerException     If partition ID <code>null</code>
     * @throws IllegalArgumentException Thrown if unknown partition ID
     */
    public IntermediateResultPartition getPartitionById(IntermediateResultPartitionID resultPartitionId) {
        // Looks ups the partition number via the helper map and returns the
        // partition. Currently, this happens infrequently enough that we could
        // consider removing the map and scanning the partitions on every lookup.
        // The lookup (currently) only happen when the producer of an intermediate
        // result cannot be found via its registered execution.
        Integer partitionNumber = partitionLookupHelper
                .get(checkNotNull(resultPartitionId, "IntermediateResultPartitionID"));
        if(partitionNumber != null) {
            return partitions[partitionNumber];
        } else {
            throw new IllegalArgumentException("Unknown intermediate result partition ID " + resultPartitionId);
        }
    }

    public int getNumberOfAssignedPartitions() {
        return partitionsAssigned;
    }

    public ResultPartitionType getResultType() {
        return resultType;
    }

    public int registerConsumer() {
        final int index = numConsumers;
        numConsumers++;

        for(IntermediateResultPartition p : partitions) {
            if(p.addConsumerGroup() != index) {
                throw new RuntimeException("Inconsistent consumer mapping between intermediate result partitions.");
            }
        }
        return index;
    }

    public int getConnectionIndex() {
        return connectionIndex;
    }

    @VisibleForTesting
    void resetForNewExecution() {
        for(IntermediateResultPartition partition : partitions) {
            partition.resetForNewExecution();
        }
    }

    @VisibleForTesting
    int getNumberOfRunningProducers() {
        return numberOfRunningProducers.get();
    }

    int incrementNumberOfRunningProducersAndGetRemaining() {
        return numberOfRunningProducers.incrementAndGet();
    }

    int decrementNumberOfRunningProducersAndGetRemaining() {
        return numberOfRunningProducers.decrementAndGet();
    }

    boolean areAllPartitionsFinished() {
        return numberOfRunningProducers.get() == 0;
    }

    @Override
    public String toString() {
        return "IntermediateResult " + id.toString();
    }
}
