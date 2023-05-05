package com.huawenjin.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.huawenjin.gmall.realtime.utils.DateFormatUtil;
import com.huawenjin.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;




//数据流： web/app  ->  Nginx  ->  日志服务器（.Log）  ->  Flume  ->  Kafka(ODS)  ->   FlinkApp  ->  Kafka(DWD)
//程序： Mock(lg.sh)   ->  Flume(f1)   ->  Kafka(ZK)  ->   BaseLogApp  ->  Kafka(ZK)

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
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


        //2.消费Kafka topic_log 主题的数据创建流
        String topic = "topic_log";
        String groupId = "base_log_app_2023";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic,groupId));


        //3.过滤掉非 Json 格式的数据 & 将每行数据转换为 Json 对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){

        };
        SingleOutputStreamOperator<JSONObject> jsonOBDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                }catch (Exception e){
                    ctx.output(dirtyTag,value);
                }
            }

        });
        //获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonOBDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>");   //生产环境可以保存到数据库中


        //4.按照Mid分组
        KeyedStream<JSONObject,String> keyedStream =  jsonOBDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //5.使用状态编程做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取 is_new 标记 & ts
                String is_new = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                //获取状态中的日期
                String lastDate = lastVisitState.value();
                //判断 is_new 标记是否为"1"
                if ("1".equals(is_new)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });


        //6.使用侧输出流进行分流处理
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> errorTag = new OutputTag<String>("error"){};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                //获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    //将数据写到 error 侧输出流
                    ctx.output(errorTag, value.toJSONString());
                }
                //移除错误信息
                value.remove("err");

                //尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    //将数据写出到 start 侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //获取公共信息 & 页面id & 时间戳
                    String common = value.getString("common");
                    JSONObject page = value.getJSONObject("page");
                    String page_id = page.getString("page_id");
                    Long ts = value.getLong("ts");

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据 & 写出到 display 侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            JSONObject displayObj = new JSONObject();
                            displayObj.put("display",display);
                            displayObj.put("common", common);
                            displayObj.put("page_id", page_id);
                            displayObj.put("ts", ts);
                            ctx.output(displayTag, displayObj.toJSONString());
                        }
                    }

                    //尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        //遍历动作数据 & 写出到 action 侧输出流
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            JSONObject actionObj = new JSONObject();
                            actionObj.put("action",action);
                            actionObj.put("common", common);
                            actionObj.put("page_id", page_id);
                            actionObj.put("ts", ts);
                            ctx.output(actionTag, actionObj.toJSONString());
                        }
                    }

                    //移除曝光和动作数据&写到页面日志主流
                    value.remove("display");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }

        });

        //7.提取各个侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        //将数据打印并写入对应的主题
        pageDS.print("Page>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>");
        displayDS.print("Display>>>>>");
        actionDS.print("Action>>>>>>>");
        errorDS.print("Error>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        //9，启动任务
        env.execute("BaseLogApp");

    }


}
