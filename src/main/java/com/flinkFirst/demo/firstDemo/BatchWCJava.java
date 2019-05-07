package com.flinkFirst.demo.firstDemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用Java API来开发FLink的批处理应用程序
 */
public class BatchWCJava {

    public static void main(String[] args) throws Exception {

        String input="file:///Users/hbin/Desktop/workspace/flinkproject";

        //step1:获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //step2:读数据
       DataSource<String> text= env.readTextFile(input);

//       text.print();  检测是否已经读到
       //step3:transform
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens=value.toLowerCase().split("\t");
                for(String token:tokens){
                    if(token.length()>0){
                        collector.collect(new Tuple2<String,Integer>(token,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();

    }
}
