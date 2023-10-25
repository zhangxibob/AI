package com.bigdata.flink.sink;

import com.bigdata.flink.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class MySQLSink extends RichSinkFunction<Tuple2<String,Integer>> {
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
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
       try {

           String name = value.f0.replaceAll("[\\[\\]]", "");
           int count = value.f1;
           System.out.println("name="+name+":count="+count);
           String sql = "select 1 from newscount " + " where name = '" + name + "'";
           String updateSql = "update newscount set count = " + count + " where name = '" + name + "'";
           String insertSql = "insert into newscount(name,count) values('" + name + "'," + count + ")";

           statement = conn.prepareStatement(sql);
           ResultSet rs = statement.executeQuery();

           if (rs.next()) {
               //更新
               statement.executeUpdate(updateSql);
           } else {
               //插入
               statement.execute(insertSql);
           }
       }catch (Exception e){
           e.printStackTrace();
       }

    }
}
