package com.bigdata.flink.stream;

import com.bigdata.flink.sink.MySQLSink;
import com.bigdata.flink.sink.MySQLSink2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class KafkaFlinkMySQL {
    public static void main(String[] args) throws Exception {
        //获取flink执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.getConfig().setAutoWatermarkInterval(200);
        //配置kafka集群参数
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hadoop1:9092,hadoop2:9092,hadoop3:9092");
        prop.setProperty("group.id","sougoulogs");

        //读取kafka数据
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("sougoulogs",new SimpleStringSchema(),prop);
        DataStream<String> stream = senv.addSource(myConsumer);

        //数据过滤
        DataStream<String> filter = stream.filter((value)->value.split(",").length==6);

        //统计新闻话题访问量
        DataStream<Tuple2<String,Integer>> newsCounts = filter.flatMap(new lineSplitter())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> t) throws Exception {
                        return t.f0;
                    }
                }).sum(1);
        newsCounts.print();
        //数据入库MySQL
        newsCounts.addSink(new MySQLSink());

        //统计每个时段新闻话题访问量
        DataStream<Tuple2<String,Integer>> periodCounts =filter.flatMap(new lineSplitter2())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> t) throws Exception {
                        return t.f0;
                    }
                }).sum(1);
        periodCounts.addSink(new MySQLSink2());

        //执行flink程序
        senv.execute("KafkaFlinkMySQL");
    }

    public static final class lineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.split(",");
            collector.collect(new Tuple2<>(tokens[2],1));
        }
    }

    public static final class lineSplitter2 implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.split(",");
            collector.collect(new Tuple2<>(tokens[0],1));
        }
    }
}
