package com.bigdata.flink.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class MyWindowFunction implements WindowFunction<Tuple3<Long,String,String>, Tuple4<String,String,String,Long>, Tuple2<String,String>, TimeWindow> {
    @Override
    public void apply(Tuple2<String, String> t2, TimeWindow window, Iterable<Tuple3<Long, String, String>> t3, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
        String audit_type = t2.f0;
        String province_code = t2.f1;
        Iterator<Tuple3<Long,String,String>> iterator =  t3.iterator();

        ArrayList<Long> list = new ArrayList<>();
        long count = 0;
        while (iterator.hasNext()){
            Tuple3<Long,String,String> it = iterator.next();
            list.add(it.f0);
            count++;
        }

        Collections.sort(list);

        Long time = list.get(list.size()-1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String insertTime = sdf.format(new Date(time));

        Tuple4<String,String,String,Long> result = new Tuple4<String,String,String,Long>(insertTime,audit_type,province_code,count);

        out.collect(result);
    }
}
