package com.flinkFirst.demo.thirdDemo;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


public class DataSetSource {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        //fromCollection(env);
        textFile(env);

    }

    public static void textFile(ExecutionEnvironment env) throws Exception{
        String filePath="file:///Users/hbin/Desktop/workspace/flinkproject/hello.txt";
        env.readTextFile(filePath).print();
        System.out.println("------------分割线------------");
        filePath="file:///Users/hbin/Desktop/workspace/flinkproject/inputs";
        env.readTextFile(filePath).print();
    }


    //定义获取数据源的方法
    public static void fromCollection(ExecutionEnvironment env) throws Exception{
        List<Integer> list=new ArrayList<>();
        for(int i=1;i<=10;i++){
            list.add(i);
        }
        //给环境传递一个集合
        env.fromCollection(list).print();
    }
}
