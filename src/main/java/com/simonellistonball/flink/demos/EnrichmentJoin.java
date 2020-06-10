package com.simonellistonball.flink.demos;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import avro.shaded.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class EnrichmentJoin {

    public static DataStream<Row> enrich(DataStream<Row> stream, DataStream<Row> enrichment, String joinKey) {

        RowTypeInfo streamType = (RowTypeInfo) stream.getType();
        int streamKeyIndex = streamType.getFieldIndex(joinKey);

        RowTypeInfo enrichType = (RowTypeInfo) enrichment.getType();
        int enrichmentKeyIndex = enrichType.getFieldIndex(joinKey);

        DataStream<Row> joined = stream.keyBy(r -> r.getField(streamKeyIndex))
                .connect(enrichment.keyBy(r -> r.getField(enrichmentKeyIndex)))
                .process(new JoinProcessor(streamType, enrichType, enrichmentKeyIndex));

        return joined;
    }

    public static class JoinProcessor extends KeyedCoProcessFunction<Object, Row, Row, Row> implements ResultTypeQueryable<Row> {

        private final RowTypeInfo streamType;
        private final RowTypeInfo enrichType;
        private final int enrichKey;

        private transient ValueState<Row> enrichState;
        private transient ListState<Row> streamState;

        public JoinProcessor(RowTypeInfo streamType, RowTypeInfo enrichType, int enrichKey) {
            this.streamType = streamType;
            this.enrichType = enrichType;
            this.enrichKey = enrichKey;
        }

        @Override
        public void processElement1(Row row, Context context, Collector<Row> out) throws Exception {
            Row enrichData = enrichState.value();
            if (enrichData != null) {
                out.collect(join(row, enrichData));
            } else {
                streamState.add(row);
            }
        }

        @Override
        public void processElement2(Row row, Context context, Collector<Row> out) throws Exception {
            enrichState.update(row);
            for (Row streamData : streamState.get()) {
                out.collect(join(streamData, row));
            }
            streamState.clear();
        }

        @Override
        public void open(Configuration conf) {
            enrichState = getRuntimeContext().getState(new ValueStateDescriptor<>("enrich", enrichType));
            streamState = getRuntimeContext().getListState(new ListStateDescriptor<>("stream", streamType));
        }

        private Row join(Row streamData, Row enrichData) {
            Row joinedRow = new Row(streamData.getArity() + enrichData.getArity() - 1);

            for (int i = 0; i < streamData.getArity(); i++) {
                joinedRow.setField(i, streamData.getField(i));
            }

            boolean shift = false;

            for (int i = 0; i < enrichData.getArity(); i++) {
                if (i == enrichKey) {
                    shift = true;
                    continue;
                }
                joinedRow.setField(streamData.getArity() + i - (shift ? 1 : 0), enrichData.getField(i));
            }
            return joinedRow;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            List<TypeInformation<?>> fieldTypes = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();

            fieldTypes.addAll(Lists.newArrayList(streamType.getFieldTypes()));
            fieldNames.addAll(Lists.newArrayList(streamType.getFieldNames()));

            for (int i = 0; i < enrichType.getArity(); i++) {
                if (i != enrichKey) {
                    fieldTypes.add(enrichType.getTypeAt(i));
                    fieldNames.add(enrichType.getFieldNames()[i]);
                }
            }

            return new RowTypeInfo(fieldTypes.toArray(new TypeInformation<?>[fieldTypes.size()]),
                    fieldNames.toArray(new String[fieldNames.size()]));
        }
    }

}
