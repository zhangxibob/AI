package com.bigdata.flink.function;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class MyFilterFunction implements FilterFunction<Tuple3<Long,String,String>> {
    @Override
    public boolean filter(Tuple3<Long, String, String> value) throws Exception {
        boolean flag = true;
        if(value.f0==0){
            flag=false;
        }
        return flag;
    }
}
