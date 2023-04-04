package com.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLUtils {

    public  static Connection getConnection() throws Exception{

        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }finally {
            System.out.println("数据库驱动加载成功");
        }
        try {
            conn = DriverManager.getConnection("jdbc:mysql://192.168.110.132:3306/hmx_flink","hmx","hmx3967");
//            conn= DriverManager.getConnection("jdbc:mysql://192.168.110.132:3306/gaspipeline?serverTimezone=Asia/Shanghai&user=root&password=910630&useUnicode=true&characterEncoding=utf-8&useSSL=false","hmx","hmx3967");
            System.out.println("数据库连接成功");
        }catch (Exception e){
            e.printStackTrace();
        }
        
        return conn;
    }

    public static void  close(AutoCloseable closeable) {

        if (null !=closeable){
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                closeable=null;
            }
        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println(getConnection());

    }
}
