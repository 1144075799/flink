package com.flinkFirst.demo.thirdDemo;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class DatasetSink {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> info=new ArrayList<Integer>();

        for(int i=1;i<=10;i++){
            info.add(i);
        }

        String filePath="file:///Users/hbin/Desktop/workspace/flinkproject/write.txt";

        DataSource<Integer> data=env.fromCollection(info);
        data.writeAsText(filePath, FileSystem.WriteMode.NO_OVERWRITE);
        env.execute("DatasetSink");
    }

}
