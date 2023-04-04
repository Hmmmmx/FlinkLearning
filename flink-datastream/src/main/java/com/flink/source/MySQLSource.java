package com.flink.source;

import com.flink.bean.Student;
import com.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLSource extends RichSourceFunction<Student> {

    Connection connection;
    PreparedStatement pstmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        pstmt = connection.prepareStatement("select * from student;");
    }

    @Override
    public void close() throws Exception {
        MySQLUtils.close(pstmt);
        MySQLUtils.close(connection);
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()){
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            Student student = new Student(id, name, age);
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {

    }
}
