package com.flink.source;

import com.flink.bean.Access;
import com.flink.bean.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkDataSourceAPP {

    public static void main(String[] args)  throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6);
//        SingleOutputStreamOperator<Integer> mapStream = source.map(x -> x + 2);
//        mapStream.print();

//        DataStreamSource<Access> source = env.addSource(new AccessSource());
//        source.print();

//        DataStreamSource<Student> studentDataStreamSource = env.addSource(new MySQLSource());
//        studentDataStreamSource.print();

        env.addSource(new MySQLSource()).print();


        env.execute();
    }
}
