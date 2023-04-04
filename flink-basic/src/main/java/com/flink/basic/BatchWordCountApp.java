package com.flink.basic;

import com.flink.basic.function.MyFlatMapFunction;
import com.flink.basic.function.MyMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Locale;

public class BatchWordCountApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("data/wc.data");

            source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    String[] splits = value.split(",");
                    for(String split:splits){
                        out.collect(Tuple2.of(split.toUpperCase(Locale.ROOT).trim(),1));
                    }
                }
            }).groupBy(0)
                    .sum(1)
                    .print();
                    
        }
}