package com.jackie.transformation;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

public class LeftOuterJoin {
    public static void main(String[] args) throws Exception {
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

        DataStream<Tuple2<Integer, String>> mapPerson = personStream.map(new MapFunction<String, Tuple2<Integer, String>>() {
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

        DataStream<Tuple3<Integer, String, String>> leftOuterStream = mapPerson.keyBy(p -> p.f0)
                .connect(mapLocation.keyBy(l -> l.f0))
                .process(new KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    private MapState<Integer, Tuple2<Integer, String>> personState;
                    private MapState<Integer, Tuple2<Integer, String>> locationState;


                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        super.open(openContext);
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Duration.ofDays(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                                .build();

                        MapStateDescriptor<Integer, Tuple2<Integer, String>> personStateDescriptor = new MapStateDescriptor<>("personState",
                                TypeInformation.of(new TypeHint<Integer>() {
                                }),
                                TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
                                }));
                        MapStateDescriptor<Integer, Tuple2<Integer, String>> locationStateDescriptor = new MapStateDescriptor<>("personState",
                                TypeInformation.of(new TypeHint<Integer>() {
                                }),
                                TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
                                }));
                        personStateDescriptor.enableTimeToLive(ttlConfig);
                        locationStateDescriptor.enableTimeToLive(ttlConfig);
                        personState = getRuntimeContext().getMapState(personStateDescriptor);
                        locationState = getRuntimeContext().getMapState(locationStateDescriptor);

                    }

                    @Override
                    public void processElement1(Tuple2<Integer, String> person, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>.Context context, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                        boolean matched = false;
                        for (Map.Entry<Integer, Tuple2<Integer, String>> location : locationState.entries()) {
                            if (person.f0.equals(location.getKey())) {
                                out.collect(new Tuple3<>(person.f0, person.f1, location.getValue().f1));
                                matched = true;
                            }
                        }
                        if (!matched) {
                            out.collect(new Tuple3<>(person.f0, person.f1, "NULL"));
                        }
                        personState.put(person.f0, person);
                    }

                    @Override
                    public void processElement2(Tuple2<Integer, String> location, KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>.Context context, Collector<Tuple3<Integer, String, String>> out) throws Exception {
                        for (Map.Entry<Integer, Tuple2<Integer, String>> person : personState.entries()) {
                            if (location.f0.equals(person.getKey())) {
                                out.collect(new Tuple3<>(person.getKey(), person.getValue().f1, location.f1));
                            }
                        }
                        locationState.put(location.f0, location);
                    }
                });

        leftOuterStream.print();

        env.execute("Left Outer Join App");
    }
}
