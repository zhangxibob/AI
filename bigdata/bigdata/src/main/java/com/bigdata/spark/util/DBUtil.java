package com.bigdata.spark.util;

import com.bigdata.spark.config.GlobalConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class DBUtil {
    private static final String connectionURL = GlobalConfig.DB_URL;
    private static final String username = GlobalConfig.USER_MAME;
    private static final String password = GlobalConfig.PASSWORD;
    //创建数据库的连接
    public static Connection getConnection() {
        try {
            Class.forName(GlobalConfig.DRIVER_CLASS);
            return   DriverManager.getConnection(connectionURL,username,password);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return null;
    }

    //关闭数据库的连接
    public static void close(ResultSet rs,Statement stmt,Connection con) throws SQLException {
        if(rs!=null)
            rs.close();
        if(stmt!=null)
            stmt.close();
        if(con!=null)
            con.close();
    }

    /**
     * 获取城市编码集合
     * @param sql
     * @return
     * @throws SQLException
     */
    public static List<String> getCityCodeList(String sql) throws SQLException {
        Connection con = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        List<String> list = new ArrayList<>();
        try {
            con=getConnection();
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            while (rs.next()){
                String cityCode = rs.getString("cityCode");
                list.add(cityCode);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            close(null,pst,con);
        }
        return list;
    }

    /**
     * 获取省份编码集合
     * @param sql
     * @return
     * @throws SQLException
     */
    public static List<String> getProvinceCodeList(String sql) throws SQLException {
        Connection con = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        List<String> list = new ArrayList<>();
        try {
            con=getConnection();
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            while (rs.next()){
                String provinceCode = rs.getString("provinceCode");
                list.add(provinceCode);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            close(null,pst,con);
        }
        return list;
    }

    /**
     * 查询广告id 列表
     * @param sql
     * @return
     * @throws SQLException
     */
    public static List<String> getAdvertiseList(String sql) throws SQLException {
        Connection con = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        List<String> list = new ArrayList<>();
        try {
            con=getConnection();
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            while (rs.next()){
                String aid = rs.getString("aid");
                list.add(aid);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            close(null,pst,con);
        }
        return list;
    }
}
