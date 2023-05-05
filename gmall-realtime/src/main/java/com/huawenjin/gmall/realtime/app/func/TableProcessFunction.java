package com.huawenjin.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huawenjin.gmall.realtime.bean.TableProcess;
import com.huawenjin.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    //创建Phoenix连接
    private Connection connection;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //{"before":null,"after":{"source_table":"aa","sink_table":"dim_aaa","sink_columns":"cv","sink_pk":"df","sink_extend":"ere"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1677399274314,"snapshot":"false","db":"gmall-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1677399274316,"transaction":null}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.校验并建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        //3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState =  context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(), tableProcess);

    }


    /***
     * 校验并建表：create table if not exists db,tn(id varchar primary key, bb varchar, cc varchar) xxx;
     * @param sinkTable         Phoenix表名
     * @param sinkColumns       Phoenix表字段
     * @param sinkPK            Phoenix表主键
     * @param sinkExtend        Phoenix表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPK, String sinkExtend){
        PreparedStatement preparedStatement = null;
        try {
            //处理特殊字段 null
            if (sinkPK == null || "".equals(sinkPK)){
                sinkPK = "id";
            }
            if (sinkExtend == null){
                sinkExtend = "";
            }

            //拼接SQL
            StringBuilder createTableSql =  new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                // 取出字段
                String column = columns[i];

                //判断是否主键
                if (sinkPK.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key");
                }else {
                    createTableSql.append(column).append(" varchar");
                }

                //判断是否最后字段
                if (i < columns.length -1){
                    createTableSql.append(",");
                }
            }

            //拼接扩展字段
            createTableSql.append(")").append(sinkExtend);

            //编译SQL
            System.out.println("建表语句为：" + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            //执行SQL
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            //释放资源
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }



    //value: {"database":"gmall-flink","table":"base_trademark","type":"insert","ts":1676775714,"xid":395,"commit":true,"data":{"id":13,"tm_name":"一加","logo_url":"/static/default.jpg"}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> out) throws Exception {

        //1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        //主流数据表名
        String table = value.getString("table");

        //广播流找到对应的维表
        TableProcess tableProcess =  broadcastState.get(table);


        if (tableProcess != null){
            //2.过滤字段
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.补充 sinkTable 并写出到流中
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);

        }else {
            System.out.println("找不到对应的key:" + table);
        }

    }


    /**
     * 过滤字段
     * @param data            {"id":13,"tm_name":"一加","logo_url":"/static/default.jpg"}
     * @param sinkColumns       "id,tm_name"
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        //切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (! columnList.contains(next.getKey())){
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }


//    //关闭 Phoenix 连接
//    @Override
//    public void close() throws Exception {
//        connection.close();
//    }
}
