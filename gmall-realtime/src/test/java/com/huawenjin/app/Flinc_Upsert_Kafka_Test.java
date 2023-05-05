package com.huawenjin.app;


import com.huawenjin.bean.WaterSensor;
import com.huawenjin.bean.WaterSensor2;
import com.huawenjin.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Flinc_Upsert_Kafka_Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境配置为Kafka主题的分区数

        //创建FlinkSQL env
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置状态过期时间
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = env.socketTextStream("node5", 8888).map(
                line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2]));
                }
        );


        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("node5", 9999).map(
                line -> {
                    String[] split = line.split(",");
                    return new WaterSensor2(split[0],
                            (split[1]),
                            Long.parseLong(split[2]));
                }
        );

        // 将流转换为动态表
        tableEnv.createTemporaryView("t1",waterSensorDS1);
        tableEnv.createTemporaryView("t2",waterSensorDS2);


        //full join    左表：OnReadAndWrite     右表：OnReadAndWrite
        Table resultTable = tableEnv.sqlQuery("select t1.id  t1_id ,t2.id  t2_id,t2.name from t1 full join t2 on t1.id=t2.id");

        tableEnv.createTemporaryView("result_table", resultTable);

        //创建UpsertKafka表
        tableEnv.executeSql(""+
                "create table upsert_test("+
                "    t1_id string,"+
                "    t2_id string,"+
                "    name string, " +
                 "   PRIMARY KEY (t1_id) NOT ENFORCED" +
                        ") "
                + MyKafkaUtil.getUpsertKafkaDDL("test")
                );

        //将数据写入Kafka
        tableEnv.executeSql("insert into upsert_test select * from result_table");


    }
}
