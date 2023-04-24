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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing {@link MapFunction MapFunctions}.
 */
@Internal
public class StreamMap<IN, OUT> extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>> implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    public StreamMap(MapFunction<IN, OUT> mapper) {
        super(mapper);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {

        // TODO_MA 注释： env.socketTextStrewam().map().keyby().sum()
        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 执行具体的逻辑转换，然后替换覆盖原来的 element
         *  1、element.getValue() 具体的数据 待处理的数据
         *  2、userFunction.map(element.getValue() 调用用户自定义的逻辑来执行抓换：  ds.map(f1) ==>  f1 = userFunction
         *     得到 map 算子处理之后的结果
         *  3、将处理得到的计算结果，覆盖原来的 element
         *  4、继续收集
         *  -
         *  如果当前这个 Operator 是一个 OperatChain 中的最后一个， 则此处的  output = RecordWriterOutput
         *  -
         *  element.getValue() 数据
         *  userFunction.map(element.getValue()) 通过map函数的逻辑，执行了一次转换
         *  element.replace(userFunction.map(element.getValue())) 复用内存
         *  X ==> Y ==> X
         */
        output.collect(element.replace(userFunction.map(element.getValue())));
    }
}
