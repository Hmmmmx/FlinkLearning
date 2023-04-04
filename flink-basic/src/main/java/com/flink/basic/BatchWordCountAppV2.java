package com.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Locale;

public class BatchWordCountAppV2 {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.readTextFile("data/wc.data");

            source.flatMap((String value, Collector<Tuple2<String,Integer>> out) -> {
                String[] splits = value.split(",");
                for(String split:splits){
                    out.collect(Tuple2.of(split.trim(),1));
                }
            }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .groupBy(0)
                    .sum(1)
                    .print();

        }
}