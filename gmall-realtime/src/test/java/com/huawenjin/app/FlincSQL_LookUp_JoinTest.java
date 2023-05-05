package com.huawenjin.app;


import com.huawenjin.bean.WaterSensor;
import com.huawenjin.bean.WaterSensor2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlincSQL_LookUp_JoinTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境配置为Kafka主题的分区数

        //创建FlinkSQL env
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TEMPORARY TABLE base_dic ( " +
                "  dic_code STRING, " +
                "  dic_name STRING, " +
                "  parent_code STRING, " +
                "  create_time STRING, " +
                "  operate_time STRING " +
                ") WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'url' = 'jdbc:mysql://node4:3306/gmall-flink?serverTimezone=UTC', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'lookup.cache.max-rows' = '10', " +       //维表数据不变 or 会改变，但是数据的准确度要求不高
                "  'lookup.cache.ttl' = '1 minute', " +
                "  'table-name' = 'base_dic', " +
                "  'username' = 'root', " +
                "  'password' = 'hua1995225' " +
                ")");

        //打印LookUp表
//        tableEnv.sqlQuery("select * from base_dic").execute().print();

        //构建事实表
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("node5", 8888).map(
                line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2]));
                }
        );

        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("vc"),
                $("ts"),
                $("pt").proctime()
        );

        tableEnv.createTemporaryView("t1",table);


        //使用事实表关联维表，打印结果
        tableEnv.sqlQuery("select  " +
                "    t1.id, " +
                "    t1.vc, " +
                "    dic.dic_name " +
                "from t1  " +
                "join base_dic FOR SYSTEM_TIME AS OF t1.pt AS dic " +
                "on t1.id = dic_code")
                .execute()
                .print();

    }
}
