package org.apache.doris.connectors.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

public class DorisDynamicTableTest {
    private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private StreamTableEnvironment tableEnv;

    @Before
    public void setup() {
        env.setParallelism(1);
        tableEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    }

    @Test
    public void testUpsertDorisConnector() throws Exception {
        String sourceTable = "create table supplier_kafka(" +
                "  s_suppkey int,\n" +
                "  s_name string,\n" +
                "  s_address string,\n" +
                "  s_city string,\n" +
                "  s_nation string,\n" +
                "  s_region string,\n" +
                "  s_phone string,\n" +
                "  PRIMARY KEY (s_suppkey) NOT ENFORCED\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'mysql_local_regex.poc',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'group-flink-sql',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'value.format' = 'debezium-json')";

        String sinkTable = "create table supplier_sink(\n" +
                "  s_suppkey int,\n" +
                "  s_name string,\n" +
                "  s_address string,\n" +
                "  s_city string,\n" +
                "  s_nation string,\n" +
                "  s_region string,\n" +
                "  s_phone string,\n" +
                "  PRIMARY KEY (s_suppkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'upsert-doris',\n" +
                "'doris.fenodes' = '192.168.0.103:8530',\n" +
                "'doris.username' = 'root',\n" +
                "'doris.password' = '',\n" +
                "'doris.table.identifier' = 'poc.dim_supplier_sink')";

        tableEnv.executeSql(sourceTable);
        tableEnv.executeSql(sinkTable);
        tableEnv.executeSql("insert into supplier_sink select * from supplier_kafka");
        env.execute(getClass().getSimpleName());
    }
}
