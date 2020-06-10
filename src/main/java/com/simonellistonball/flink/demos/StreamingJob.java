/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.simonellistonball.flink.demos;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.io.Resources;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Streams;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.simonellistonball.flink.demos.EnrichmentJoin.enrich;
import static com.simonellistonball.flink.demos.Utils.readKafkaProperties;

/**
 * CDC processing demo
 */
public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private static final TypeInformation<Row> STATE_TYPE = Types.ROW_NAMED(new String[]{"StateProvinceID", "CountryRegionCode", "Name"}, Types.INT, Types.STRING, Types.STRING);
    private static final TypeInformation<Row> ADDRESS_TYPE = Types.ROW_NAMED(new String[]{"AddressID", "AddressLine1", "City", "StateProvinceID", "PostalCode", "ModifiedDate"},
            Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING);
    private static final TypeInformation<Row> ORDER_TYPE = Types.ROW_NAMED(new String[]{"SalesOrderId", "OrderDate", "DueDate", "ShipDate", "Status", "ShipToAddressId", "SubTotal", "TaxAmt", "Freight", "TotalDue"},
            Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.INT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.registerFunction("parseTs", new DateParser());

        Map<String, String> kafkaConnectorProperties = readKafkaProperties(params).entrySet().stream().collect(
                Collectors.toMap(e -> "connector.properties." + e.getKey(), e -> e.getValue().toString()));

        tableEnv.sqlUpdate(createPreciselyTable(
                "cdc_States",
                "cdc-states",
                STATE_TYPE,
                kafkaConnectorProperties));

        DataStream<Row> statesStream = tableEnv.toAppendStream(tableEnv.sqlQuery(loadSql("sql/states.sql")), Row.class).keyBy("StateProvinceID");

        tableEnv.sqlUpdate(createPreciselyTable(
                "cdc_Address",
                "cdc-addresses",
                ADDRESS_TYPE,
                kafkaConnectorProperties));

        DataStream<Row> addressesStream = tableEnv.toAppendStream(tableEnv.sqlQuery(loadSql("sql/address.sql")), Row.class).keyBy("StateProvinceID");

        tableEnv.sqlUpdate(createPreciselyTable(
                "cdc_Orders",
                "cdc-orders",
                ORDER_TYPE,
                kafkaConnectorProperties));

        DataStream<Row> ordersStream = tableEnv.toAppendStream(tableEnv.sqlQuery(loadSql("sql/orders.sql")), Row.class);

        // Do the enrichments in DataTable API
        DataStream<Row> addressWithState = enrich(addressesStream, statesStream, "StateProvinceID");
        DataStream<Row> ordersWithAddress = enrich(ordersStream, addressWithState, "AddressId");

        RowTypeInfo rowTypeInfo = (RowTypeInfo) ordersWithAddress.getType();
        final int orderDateIndex = rowTypeInfo.getFieldIndex("OrderAsOf");
        LOG.info(String.format("Index of Order Date field %d", orderDateIndex));

        DataStream<Row> timedOrdersWithAddress = ordersWithAddress.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(10)) {
                @Override
                public long extractTimestamp(Row row) {
                    return (long) row.getField(orderDateIndex);
                }
            }
        );

        tableEnv.createTemporaryView("ordersWithAddress", timedOrdersWithAddress);

        tableEnv.sqlUpdate(createOutputTable("output", "precisely_results", kafkaConnectorProperties));
        tableEnv.sqlUpdate(loadSql("sql/output.sql"));

        env.execute("Precisely Demo Job");

    }

    private static String loadSql(String fileName) throws IOException {
        URL url = Resources.getResource(fileName);
        return Resources.toString(url, StandardCharsets.UTF_8);
    }

    private static String createOutputTable(String table, String topic, Map<String, String> connectorProperties) {
        return String.format("CREATE TABLE %s (" +
                        "OrderPeriod TIMESTAMP(3)," +
                        "TotalDue FLOAT," +
                        "Country STRING," +
                        "State STRING" +
                        ") WITH (%s)",
                table,
                createTableOptions(topic, connectorProperties));
    }

    private static String createPreciselyTable(String table, String topic, TypeInformation<Row> fields, Map<String, String> connectorProperties) {
        RowTypeInfo rowTypeInfo = ((RowTypeInfo) fields);

        String row = Streams.zip(
                Arrays.stream(rowTypeInfo.getFieldNames()),
                Arrays.stream(rowTypeInfo.getFieldTypes()),
                (name, type) -> name + " " + typeToSql(type))
                .collect(Collectors.joining(","));

        LOG.info(rowTypeInfo.getFieldNames().toString());
        LOG.info(rowTypeInfo.getFieldTypes().toString());

        String out = String.format("CREATE TABLE %s (" +
                        "sv_manip_type STRING," +
                        "sv_trans_id BIGINT," +
                        "sv_trans_row_seq INT," +
                        "sv_sending_table STRING," +
                        "sv_trans_timestamp STRING," +
                        "sv_trans_username STRING," +
                        "sv_program_name STRING," +
                        "sv_job_name STRING," +
                        "sv_job_user STRING," +
                        "sv_job_number STRING," +
                        "sv_op_timestamp STRING," +
                        "sv_file_member STRING," +
                        "sv_receiver_library STRING," +
                        "sv_receiver_name STRING," +
                        "sv_journal_seqno STRING," +
                        "after_image ROW<%s>" +
                        ") WITH (%s)",
                table,
                row,
                createTableOptions(topic, connectorProperties));
        LOG.info(out);
        return out;
    }

    private static String createTableOptions(String topic, Map<String, String> connectorProperties) {
        return Stream.concat(
                new HashMap<String, String>() {{
                    put("connector.type", "kafka");
                    put("connector.version", "universal");
                    put("connector.startup-mode", "earliest-offset");
                    put("connector.properties.group.id", "cdc-raw-read");
                    put("connector.properties.client.id", "precisely_processed");
                    put("format.type", "json");
                    put("connector.topic", topic);
                }}.entrySet().stream(),
                connectorProperties.entrySet().stream())
                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue())).collect(Collectors.joining(","));
    }

    private static String typeToSql(TypeInformation<?> type) {
        switch (type.getTypeClass().getTypeName()) {
            case "java.lang.Integer":
                return "INT";
            case "java.lang.Float":
                return "FLOAT";
            default:
                return "STRING";
        }
    }
}
