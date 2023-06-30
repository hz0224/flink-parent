package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketWordCount {
    public static void main(String[] args) throws Exception {

        //1 创建StreamExecutionEnvironment
        /*************************************************
         *  注释： 第一步： 构建　StreamExecutionEnvironment
         *  这行代码会返回一个可用的执行环境。执行环境是整个flink程序执行的上下文，记录了相关配置（如并行度等），
         *  并提供了一系列方法，如读取输入流的方法，以及真正开始运行整个代码 的execute方法等。
         *
         *  这个对象的内部，有一个很重要的成员变量： List<Transformation<?>> transformations
         *     1、userFunction（算子）
         *     2、StrewamOperator
         *     3、Transformation
         *     4、StreamNode（StreamGraph中的顶点， 因此一个算子一个顶点）
         *     这4者之间都是一一对应的。
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2 读取数据
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);

        //3 flatMap      注意内部：Function ==> StreamOperator ==> Transformation
        SingleOutputStreamOperator<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        });

        //4 map
        SingleOutputStreamOperator<Tuple2<String, Long>> mapStream = flatMapStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });

        //5 分组聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = mapStream.keyBy(t -> t.f0).sum(1);

        //6 输出
        resultStream.print();

        //打印 StreamGraph
        String streamingPlanAsJSON = env.getStreamGraph().getStreamingPlanAsJSON();
        System.out.println(streamingPlanAsJSON);

        //打印 ExecutionPlan
        String executionPlan = env.getExecutionPlan();
        System.out.println(executionPlan);

        /*************************************************
         *  提交执行
         *  1、生成 StreamGraph。代表程序的拓扑结构，是从用户代码直接生成的图。
         *  2、生成 JobGraph。这个图是要交给 flink 去生成 task 的图。
         *  3、生成一系列配置。
         *  4、将 JobGraph 和配置交给 flink 集群去运行。如果不是本地运行的话，还会把 jar 文件通过网络发给其他节点。
         *  5、以本地模式运行的话，可以看到启动过程，如启动性能度量、web 模块、JobManager、 ResourceManager、taskManager 等等
         *  6、启动任务。值得一提的是在启动任务之前，先启动了一个用户类加载器，这个类加载器可以用来做一些在运行时动态加载类的工作。
         */

        //7 提交任务
        env.execute("wordCount");
    }
}
