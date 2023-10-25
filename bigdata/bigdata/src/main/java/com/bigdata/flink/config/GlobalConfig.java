package com.bigdata.flink.config;
/**
 * @author   杨俊
 * @contact  咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
public class GlobalConfig {
    public static final String DRIVER_CLASS="com.mysql.jdbc.Driver";

    public static final String DB_URL="jdbc:mysql://hadoop1:3306/live?useUnicode=true&characterEncoding=UTF8&useSSL=false";

    public static final String USER_MAME = "hive";

    public static final String PASSWORD = "hive";

    public static String AUDITINSERTSQL = "insert into  auditcount (time,audit_type,province_code,count) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE time=VALUES(time),audit_type=VALUES(audit_type),province_code=VALUES(province_code),count=VALUES(count)";
}
