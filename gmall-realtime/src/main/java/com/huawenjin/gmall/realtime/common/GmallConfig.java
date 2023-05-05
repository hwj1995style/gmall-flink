package com.huawenjin.gmall.realtime.common;

public class GmallConfig {

    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node4,node5,node6:2181";
}
