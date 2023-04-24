/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.streaming.runtime.tasks.WatermarkGaugeExposingOutput;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link Output} that sends data using a {@link RecordWriter}.
 */
@Internal
public class RecordWriterOutput<OUT> implements WatermarkGaugeExposingOutput<StreamRecord<OUT>> {

    private RecordWriter<SerializationDelegate<StreamElement>> recordWriter;

    private SerializationDelegate<StreamElement> serializationDelegate;

    private final StreamStatusProvider streamStatusProvider;

    private final OutputTag outputTag;

    private final WatermarkGauge watermarkGauge = new WatermarkGauge();

    @SuppressWarnings("unchecked")
    public RecordWriterOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter,
            TypeSerializer<OUT> outSerializer, OutputTag outputTag, StreamStatusProvider streamStatusProvider) {

        checkNotNull(recordWriter);
        this.outputTag = outputTag;
        // generic hack: cast the writer to generic Object type so we can use it
        // with multiplexed records and watermarks
        this.recordWriter = (RecordWriter<SerializationDelegate<StreamElement>>) (RecordWriter<?>) recordWriter;

        TypeSerializer<StreamElement> outRecordSerializer = new StreamElementSerializer<>(outSerializer);

        if(outSerializer != null) {
            serializationDelegate = new SerializationDelegate<StreamElement>(outRecordSerializer);
        }

        this.streamStatusProvider = checkNotNull(streamStatusProvider);
    }

    @Override
    public void collect(StreamRecord<OUT> record) {
        if(this.outputTag != null) {
            // we are not responsible for emitting to the main output.
            return;
        }

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 执行写出！
         *  每个Task在初始化的时候，都启动了一个 recordreader 也启动了一个 recordWriter
         */
        pushToRecordWriter(record);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        if(OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
            pushToRecordWriter(record);
        }
    }

    private <X> void pushToRecordWriter(StreamRecord<X> record) {

        // TODO_MA 注释： 此处进行了 StrewamRecord 的序列化
        serializationDelegate.setInstance(record);

        try {

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 写出去
             *  recordWriter = ChannelSelectorRecordWriter
             *  类似于 MapReduce 或者 Spark 中的 Partitioner：  用来决定到底吧数据写出到哪一个 ResultSubPartition
             */
            recordWriter.emit(serializationDelegate);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： ChannelSelectorRecordWriter
             *  Spark MapReduce 中的 Partitioner: 用来觉得一条数据到底被发送到下游的那个Task
             */

        } catch(Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void emitWatermark(Watermark mark) {
        watermarkGauge.setCurrentWatermark(mark.getTimestamp());
        serializationDelegate.setInstance(mark);

        if(streamStatusProvider.getStreamStatus().isActive()) {
            try {
                recordWriter.broadcastEmit(serializationDelegate);
            } catch(Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public void emitStreamStatus(StreamStatus streamStatus) {
        serializationDelegate.setInstance(streamStatus);

        try {
            recordWriter.broadcastEmit(serializationDelegate);
        } catch(Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        serializationDelegate.setInstance(latencyMarker);

        try {
            recordWriter.randomEmit(serializationDelegate);
        } catch(Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 发送 CheckpointBarrier 消息
         */
        recordWriter.broadcastEvent(event, isPriorityEvent);
    }

    public void flush() throws IOException {
        recordWriter.flushAll();
    }

    @Override
    public void close() {
        recordWriter.close();
    }

    @Override
    public Gauge<Long> getWatermarkGauge() {
        return watermarkGauge;
    }
}
