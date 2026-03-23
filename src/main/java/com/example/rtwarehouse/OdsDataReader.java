package com.example.rtwarehouse;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * ODS层数据读取 - 从MySQL CDC读取3张表
 * 直接运行即可看到数据
 */
public class OdsDataReader {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // MySQL CDC Source
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("8.222.156.182")
                .port(1001)
                .databaseList("statics")
                .tableList(
                        "statics.device_users_relations_flat",
                        "statics.game_record",
                        "statics.user_online_status_record"
                )
                .username("root")
                .password("root")
                .serverId("5400-5500")
                .serverTimeZone("Asia/Jakarta")
                .startupOptions(StartupOptions.latest())  // 只读增量，若改成 initial() 可以读全量
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // 读取数据并打印        
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source").print("ODS");

        // 执行
        env.execute("ODS Data Reader");
    }
}
