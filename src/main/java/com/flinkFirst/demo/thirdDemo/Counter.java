package com.flinkFirst.demo.thirdDemo;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;


public class Counter {

    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data=env.fromElements("hadoop","spark","flink","pyspark","storm");

        DataSet<String> info=data.map(new RichMapFunction<String, String>() {

            LongCounter counter=new LongCounter();          //定义一个累加器

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                getRuntimeContext().addAccumulator("ele-counts-Java",counter);  //注册一个累加器
            }

            @Override
            public String map(String s) throws Exception {
                counter.add(1);                                     //累加器加一
                return s;
            }
        });

        String filePath="file:///Users/hbin/Desktop/workspace/flinkproject/write1.txt";
        info.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(3);
        JobExecutionResult jobResult=env.execute("Counter");

        long num=jobResult.getAccumulatorResult("ele-counts-Java");

        System.out.println("Counter="+num);



    }

}
