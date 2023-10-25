package com.bigdata.spark.config;

import java.io.Serializable;

/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class GlobalConfig implements Serializable {
    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://hadoop1:3306/advertise?useUnicode=true&characterEncoding=UTF8&useSSL=false";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "hive";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "hive";
}
