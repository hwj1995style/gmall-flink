package com.huawenjin.gmall.realtime.app.dwd.db;

import com.huawenjin.gmall.realtime.utils.MyKafkaUtil;
import com.huawenjin.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;



//数据流： Web/app  ->  Nginx  -> 业务服务器（Mysql)  ->  Maxwell  ->  Kafka(ODS)  -> FlinkApp  ->   Kafka(DWD)
//程序： Mock   ->   Mysql  ->  Maxwell   ->  Kafka(ZK)  ->  DwdTradeCartAdd  ->  Kafka(ZK)
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境配置为Kafka主题的分区数

        //1.1 开启CkeckPoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);   // 5 min一次
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);  //同时共存的 CheckPoint 数量
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));   // 失败重启策略

        //1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node4:8020/gmall-flink/ck");
        System.setProperty("HADOOP_USER_NAME","huawenjin");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL 方式读取 topic_db 主题的数据创建表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add_2023"));


        //TODO 3.过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("select " +
                "     `data`['id'] id            " +
                "    ,`data`['user_id'] user_id       " +
                "    ,`data`['sku_id'] sku_id        " +
                "    ,`data`['cart_price'] cart_price    " +
                "    ,if(`type` = 'insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num       " +
                "    ,`data`['img_url'] img_url       " +
                "    ,`data`['sku_name']  sku_name      " +
                "    ,`data`['is_checked'] is_checked    " +
                "    ,`data`['create_time'] create_time   " +
                "    ,`data`['operate_time'] operate_time  " +
                "    ,`data`['is_ordered'] is_ordered    " +
                "    ,`data`['order_time'] order_time    " +
                "    ,`data`['source_type'] source_type   " +
                "    ,`data`['source_id'] source_id     " +
                "    ,pt " +
                "from topic_db  " +
                "where `database` = 'gmall-flink' " +
                "and `table` = 'cart_info' " +
                "and (`type` = 'insert' " +
                "or (`type` = 'update' and `old`['sku_num'] is not null " +
                "    and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                "    ))");

        //将加购表转换为流并打印测试
//        tableEnv.toAppendStream(cartAddTable, Row.class)
//                .print(">>>>");
        tableEnv.createTemporaryView("cart_info_table",cartAddTable);

        //TODO 4.读取Mysql 的 base_dic表作为LookUp 表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //TODO 5.关联两张表
        Table cartAddWithDicTable = tableEnv.sqlQuery("select " +
                "    t1.id " +
                "    ,t1.user_id " +
                "    ,t1.sku_id " +
                "    ,t1.cart_price " +
                "    ,t1.sku_num " +
                "    ,t1.img_url " +
                "    ,t1.sku_name " +
                "    ,t1.is_checked " +
                "    ,t1.create_time " +
                "    ,t1.operate_time " +
                "    ,t1.is_ordered " +
                "    ,t1.order_time " +
                "    ,t1.source_type source_type_id " +
                "    ,t2.dic_name source_type_name " +
                "    ,t1.source_id " +
                "from cart_info_table t1  " +
                "inner join base_dic FOR SYSTEM_TIME AS OF t1.pt AS t2 " +
                "on t1.source_type = t2.dic_code");

        tableEnv.createTemporaryView("cart_add_dic_table",cartAddWithDicTable);

        //TODO 6.使用DDL 方式创建加购事实表
        tableEnv.executeSql("create table dwd_trade_cart_add " +
                "( " +
                "     `id`                 STRING " +
                "    ,`user_id`            STRING " +
                "    ,`sku_id`             STRING " +
                "    ,`cart_price`         STRING " +
                "    ,`sku_num`            STRING " +
                "    ,`img_url`            STRING " +
                "    ,`sku_name`           STRING " +
                "    ,`is_checked`         STRING " +
                "    ,`create_time`        STRING " +
                "    ,`operate_time`       STRING " +
                "    ,`is_ordered`         STRING " +
                "    ,`order_time`         STRING " +
                "    ,`source_type_id`     STRING " +
                "    ,`source_type_name`   STRING " +
                "    ,`source_id`          STRING " +
                " " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        //TODO 7.将数据写出
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from cart_add_dic_table")
                .print();

        //TODO 7.启动任务
        env.execute("DwdTradeCartAdd");

    }
}
