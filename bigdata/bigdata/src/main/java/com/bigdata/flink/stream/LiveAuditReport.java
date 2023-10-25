package com.bigdata.flink.stream;

import com.bigdata.flink.function.MyFilterFunction;
import com.bigdata.flink.function.MyMapFunction;
import com.bigdata.flink.function.MyWindowFunction;
import com.bigdata.flink.sink.AuditMySQLSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class LiveAuditReport {
    public static void main(String[] args) throws Exception {
        //获取flink执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        senv.setParallelism(3);

        //启用checkpoint容错
        senv.enableCheckpointing(60000);
        senv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        senv.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        senv.getCheckpointConfig().setCheckpointTimeout(60000);
        senv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        senv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置状态后端
        //设置使用ROCKsDB
        //senv.setStateBackend(new EmbeddedRocksDBStateBackend());
        //设置checkpoint Storage
        //senv.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://mycluster/flink/checkpoints"));
        //配置Kafka
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hadoop1:9092,hadoop2:9092,hadoop3:9092");
        prop.setProperty("group.id","liveAuditLog");
        String inputTopic = "liveAuditLog";

        //构造flink 消费者
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(inputTopic,new SimpleStringSchema(),prop);
        //添加kafka source
        DataStreamSource<String> data = senv.addSource(myConsumer);

        //解析数据
        DataStream<Tuple3<Long,String,String>> parse =  data.map(new MyMapFunction());

        //数据过滤
        DataStream<Tuple3<Long,String,String>> filter = parse.filter(new MyFilterFunction());

        //保存迟到的数据
        OutputTag<Tuple3<Long,String,String>> outputTag = new OutputTag<Tuple3<Long,String,String>>("late-data"){};

        //窗口计算
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> result = filter.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <Tuple3<Long,String,String>>forBoundedOutOfOrderness(Duration.ofMillis(10000L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, String, String>>() {
                            @Override
                            public long extractTimestamp(Tuple3<Long, String, String> event, long l) {
                                return event.f0;
                            }
                        })
        ).keyBy(new KeySelector<Tuple3<Long, String, String>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<Long, String, String> t3) throws Exception {
                return new Tuple2<String,String>(t3.f1,t3.f2);
            }
        }).window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(outputTag)
                .apply(new MyWindowFunction());

        //获取迟到的数据
        DataStream<Tuple3<Long,String,String>> sideOutput = result.getSideOutput(outputTag);

        //配置Kafka
        Properties outprop = new Properties();
        outprop.setProperty("bootstrap.servers","hadoop1:9092,hadoop2:9092,hadoop3:9092");
        outprop.setProperty("transaction.timeout.ms",60000*10+"");
        String outputTopic = "lateLog";
        String brokerList = "hadoop1:9092,hadoop2:9092,hadoop3:9092";


        //构造Flink 生产者
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(brokerList,outputTopic,new SimpleStringSchema());

        //将迟到数据写入kafka
        sideOutput.map(new MapFunction<Tuple3<Long, String, String>, String>() {
            @Override
            public String map(Tuple3<Long, String, String> t3) throws Exception {
                return t3.f0+","+t3.f1+","+t3.f2;
            }
        }).addSink(myProducer);


        //审计指标数据入库MySQL
        result.addSink(new AuditMySQLSink());

        senv.execute("LiveAuditReport");
    }
}
