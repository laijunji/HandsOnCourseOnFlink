package com.jackie.transformation;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class InnerJoin {
    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        config.set(RestOptions.PORT,8081);
        config.set(RestOptions.BIND_ADDRESS,"localhost");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        final FileSource<String> personSource =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("D:\\github\\GitHub\\HandsOnCourseOnFlink\\src\\main\\resources\\person"))
                        .build();
        final DataStream<String> personStream =
                env.fromSource(personSource, WatermarkStrategy.noWatermarks(), "file-source");

        SingleOutputStreamOperator<Tuple2<Integer, String>> mapPerson = personStream.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(Integer.valueOf(words[0]), words[1]);
            }
        });

        final FileSource<String> locationSource =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("D:\\github\\GitHub\\HandsOnCourseOnFlink\\src\\main\\resources\\location"))
                        .build();
        final DataStream<String> locationStream =
                env.fromSource(locationSource, WatermarkStrategy.noWatermarks(), "file-source");

        DataStream<Tuple2<Integer, String>> mapLocation = locationStream.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(Integer.valueOf(words[0]), words[1]);
            }
        });

        DataStream<Tuple3<Integer, String, String>> joined = mapPerson.keyBy(i -> i.f0)
                .connect(mapLocation.keyBy(j -> j.f0))
                .flatMap(new RichCoFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    private ListState<Tuple2<Integer, String>> personState;
                    private ListState<Tuple2<Integer, String>> locationState;

                    @Override
                    public void flatMap1(Tuple2<Integer, String> person, Collector<Tuple3<Integer, String, String>> collector) throws Exception {
                        HashMap<Integer, Tuple2<Integer, String>> locationMap = new HashMap<>();
                        for (Tuple2<Integer, String> location : locationState.get()) {
                            locationMap.put(location.f0, location);
                        }
                        ;
                        if (!locationMap.isEmpty()) {
                            for (Map.Entry<Integer, Tuple2<Integer, String>> entry : locationMap.entrySet()) {
                                if (person.f0.equals(entry.getKey())) {
                                    collector.collect(new Tuple3<>(person.f0, person.f1, entry.getValue().f1));
                                } else {
                                    personState.add(person);
                                }
                            }
                        } else {
                            personState.add(person);
                        }
                    }

                    @Override
                    public void flatMap2(Tuple2<Integer, String> location, Collector<Tuple3<Integer, String, String>> collector) throws Exception {
                        HashMap<Integer, Tuple2<Integer, String>> personMap = new HashMap<>();
                        for (Tuple2<Integer, String> person : personState.get()) {
                            personMap.put(person.f0, person);
                        }
                        ;
                        if (!personMap.isEmpty()) {
                            for (Map.Entry<Integer, Tuple2<Integer, String>> entry : personMap.entrySet()) {
                                if (location.f0.equals(entry.getKey())) {
                                    collector.collect(new Tuple3<>(entry.getKey(), entry.getValue().f1, location.f1));
                                } else {
                                    locationState.add(location);
                                }
                            }
                        } else {
                            locationState.add(location);
                        }
                    }

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        super.open(openContext);
                        personState = getRuntimeContext().getListState(new ListStateDescriptor<>("personState",
                                TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
                                })));

                        locationState = getRuntimeContext().getListState(new ListStateDescriptor<>("locationState",
                                TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
                                })));

                    }
                });

        joined.print();

        env.execute("Join example");
    }
}
