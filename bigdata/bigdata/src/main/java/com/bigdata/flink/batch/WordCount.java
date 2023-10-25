package com.bigdata.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataSet<String> source = benv.readTextFile(args[0]);

        DataSet<Tuple2<String,Integer>> result = source.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);

        result.writeAsText(args[1]);

        benv.execute("WordCount");
    }

    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] arr = s.split("\\s+");
            for(String key:arr){
                if(key.length()>0){
                    collector.collect(new Tuple2<>(key,1));
                }
            }
        }
    }
}
