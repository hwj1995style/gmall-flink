package com.huawenjin.app;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Properties;

public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境配置为Kafka主题的分区数

        Properties prop = new Properties();
        prop.setProperty("useSSL","false");
        prop.setProperty("verifyServerCertificate","false");
        MySqlSource<String> mySqlSource =  MySqlSource.<String>builder()
                .hostname("node4")
                .port(3306)
                .username("root")
                .password("hua1995225")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .debeziumProperties(prop)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");

        mysqlSourceDS.print(">>>>>>");
        env.execute();
   }

}
