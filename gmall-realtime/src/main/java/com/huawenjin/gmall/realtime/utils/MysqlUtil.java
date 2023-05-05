package com.huawenjin.gmall.realtime.utils;

public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {

        return "create table `base_dic`( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {

        String ddl = "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://node4:3306/gmall-flink?serverTimezone=UTC', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '10', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = 'hua1995225', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
        return ddl;
    }

}
