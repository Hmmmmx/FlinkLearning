package com.flink.basic;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Parameter;

public class FlinkSocketWCApp {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 9999);

//        String hostname;
//        int port;
//        try {
//            ParameterTool params = ParameterTool.fromArgs(args);
//            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
//            port = params.getInt("port");
//        } catch (Exception var6) {
//            System.err.println("No port specified. Please run 'SocketWindowWordCount --hostname <hostname> --port <port>', where hostname (localhost by default) and port is the address of the text server");
//            System.err.println("To start a simple text server, run 'netcat -l <port>' and type the input text into the command line");
//            return;
//        }

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.110.132", 9527);

//        DataStreamSource<String> source = env.socketTextStream(hostname,port);

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
