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

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

    private final ResultPartitionProvider partitionProvider;

    private final TaskEventPublisher taskEventPublisher;

    private final PartitionRequestQueue outboundQueue;

    PartitionRequestServerHandler(ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher,
            PartitionRequestQueue outboundQueue) {

        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
        this.outboundQueue = outboundQueue;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();

            // ----------------------------------------------------------------
            // Intermediate result partition requests
            // ----------------------------------------------------------------

            // TODO_MA 注释： 如果 NettyClient 发送建立连接请求过来
            if(msgClazz == PartitionRequest.class) {
                PartitionRequest request = (PartitionRequest) msg;

                LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

                try {

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 先创建一个 CreditBasedSequenceNumberingViewReader
                     *  基于信用： 做反压的
                     *  上游的Task把数据发送给下游Task，下游Task接收成功了，需要返回一个反馈
                     */
                    NetworkSequenceViewReader reader;
                    reader = new CreditBasedSequenceNumberingViewReader(request.receiverId, request.credit,
                            outboundQueue);

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 在创建 ResultSubpartitionView = PipelinedSubpartitionView
                     *  是用来消费数据的
                     *  也是创建一个view，这个时候远程通信，所这个 view的内部，是要发送数据到远程 Task 的
                     */
                    reader.requestSubpartitionView(partitionProvider, request.partitionId, request.queueIndex);

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 将创建好的： CreditBasedSequenceNumberingViewReader 加入到 allReaders Map 中
                     */
                    outboundQueue.notifyReaderCreated(reader);

                } catch(PartitionNotFoundException notFound) {
                    respondWithError(ctx, notFound, request.receiverId);
                }
            }
            // ----------------------------------------------------------------
            // Task events
            // ----------------------------------------------------------------
            else if(msgClazz == TaskEventRequest.class) {
                TaskEventRequest request = (TaskEventRequest) msg;

                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                if(!taskEventPublisher.publish(request.partitionId, request.event)) {
                    respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."),
                            request.receiverId);
                }
            } else if(msgClazz == CancelPartitionRequest.class) {
                CancelPartitionRequest request = (CancelPartitionRequest) msg;
                outboundQueue.cancel(request.receiverId);
            } else if(msgClazz == CloseRequest.class) {
                outboundQueue.close();
            } else if(msgClazz == AddCredit.class) {
                AddCredit request = (AddCredit) msg;
                outboundQueue
                        .addCreditOrResumeConsumption(request.receiverId, reader -> reader.addCredit(request.credit));
            }

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 恢复消费
             */
            else if(msgClazz == ResumeConsumption.class) {
                ResumeConsumption request = (ResumeConsumption) msg;
                outboundQueue
                        .addCreditOrResumeConsumption(request.receiverId, NetworkSequenceViewReader::resumeConsumption);
            } else {
                LOG.warn("Received unexpected client request: {}", msg);
            }
        } catch(Throwable t) {
            respondWithError(ctx, t);
        }
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
        LOG.debug("Responding with error: {}.", error.getClass());

        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
    }
}
