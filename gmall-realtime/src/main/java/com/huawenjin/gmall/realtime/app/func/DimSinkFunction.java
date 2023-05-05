package com.huawenjin.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.huawenjin.gmall.realtime.utils.DruidDSUtil;
import com.huawenjin.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private  DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }


    //value: {"database":"gmall-flink","table":"base_trademark","type":"insert","ts":1676775714,"xid":395,"commit":true,"data":{"id":13,"tm_name":"一加","logo_url":"/static/default.jpg"}
    // ,"sinkTable":"xxxx"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
         DruidPooledConnection druidPooledConnection =  druidDataSource.getConnection();

        //写出数据
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");

        // 写出数据失败会导致任务终止
        PhoenixUtil.upsertValues(druidPooledConnection, sinkTable, data);

        //归还连接
        druidDataSource.close();
    }
}
