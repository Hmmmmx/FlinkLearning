package com.flink.basic;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.omg.CORBA.Environment;

public class StreamWCApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("data/wc.data");

        source.flatMap((String value, Collector<Tuple2<String,Integer>> out) -> {
            String[] splits = value.split(",");
            for(String split:splits){
                out.collect(Tuple2.of(split.trim(),1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

        try {
            env.execute("Test1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
