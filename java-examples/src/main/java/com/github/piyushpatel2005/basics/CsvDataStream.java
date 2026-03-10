package com.github.piyushpatel2005.basics;

import com.github.piyushpatel2005.basics.domain.SalesOrder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SerializableFunction;

public class CsvDataStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = mapper -> mapper
                .schemaFor(SalesOrder.class)
                .withSkipFirstDataRow(true);

        // Read the CSV file
//        CsvReaderFormat<SalesOrder> csvFormat = CsvReaderFormat
//                .forPojo(SalesOrder.class);
        CsvReaderFormat<SalesOrder> csvFormat = CsvReaderFormat
                .forSchema(
                        () -> new CsvMapper(), schemaGenerator, TypeInformation.of(SalesOrder.class)
                );
        FileSource<SalesOrder> csvSource = FileSource
                .forRecordStreamFormat(csvFormat, new Path("input/sales_dataset.csv"))
                .build();
        DataStream<SalesOrder> salesStream = env.fromSource(csvSource, WatermarkStrategy.noWatermarks(), "csv-sales-source");

        salesStream.print();
        env.execute("Read CSV file");
    }
}
