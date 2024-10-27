package com.jackie.transformation;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WordCount {
    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        config.set(RestOptions.PORT,8081);
        config.set(RestOptions.BIND_ADDRESS,"localhost");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        final FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("D:\\github\\GitHub\\HandsOnCourseOnFlink\\src\\main\\resources\\wc.txt"))
                        .build();
        final DataStream<String> TextStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        DataStream<String> filtered = TextStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("N");
            }
        });

        DataStream<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        DataStream<Tuple2<String, Integer>> counts = tokenized.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> t) throws Exception {
                return t.f0;
            }
        }).sum(1);

        counts.print();


        env.execute("Word Count Example");




    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            return new Tuple2<String, Integer>(s, Integer.valueOf(1));
        }
    }
}
