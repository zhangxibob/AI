package com.bigdata.flink.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class MyMapFunction implements MapFunction<String, Tuple3<Long,String,String>> {
    @Override
    public Tuple3<Long, String, String> map(String value) throws Exception {

        JSONObject jsonObject = JSON.parseObject(value);

        String audit_type = jsonObject.getString("audit_type");
        String province_code = jsonObject.getString("province_code");
        String time = jsonObject.getString("audit_time");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(time);
        long audit_time = date.getTime();
        return new Tuple3<>(audit_time,audit_type,province_code);
    }
}
