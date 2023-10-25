package com.bigdata.flink.sink;

import com.bigdata.flink.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class AuditMySQLSink extends RichSinkFunction<Tuple4<String,String,String,Long>> {
    private Connection conn;
    private PreparedStatement statement;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GlobalConfig.DRIVER_CLASS);
        conn = DriverManager.getConnection(GlobalConfig.DB_URL,GlobalConfig.USER_MAME,GlobalConfig.PASSWORD);
    }

    @Override
    public void close() throws Exception {
        if(statement !=null){
            statement.close();
        }
        if(conn!=null){
            conn.close();
        }
    }

    @Override
    public void invoke(Tuple4<String,String,String,Long> value, Context context) throws Exception {
        try {
            String time = value.f0;
            String audit_type = value.f1;
            String province_code = value.f2;
            long count = value.f3;
            System.out.println("province_code="+province_code);
            statement = conn.prepareStatement(GlobalConfig.AUDITINSERTSQL);
            statement.setString(1,time);
            statement.setString(2,audit_type);
            statement.setString(3,province_code);
            statement.setLong(4,count);
            statement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
