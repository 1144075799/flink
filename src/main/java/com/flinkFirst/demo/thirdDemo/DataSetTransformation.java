package com.flinkFirst.demo.thirdDemo;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


import java.util.ArrayList;
import java.util.List;

public class DataSetTransformation  {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        //mapFunction(env);
        //filterFunction(env);
        //mapPartitionFunction(env);
        //firstFunction(env);
        //flatMapFunction(env);
        //distinctFunction(env);
        //joinFunction(env);
        //outJoinFunction(env);
        //crossFunction(env);
        rangePattitionFunction(env);
    }


    public static void rangePattitionFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(5, "猪头户"));

        DataSource<Tuple2<Integer,String>> data=env.fromCollection(info1);

        data.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, String>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> iterable, Collector<String> collector) throws Exception {
                String connection=DBUtils.getConection();
                System.out.println("content="+connection);
                DBUtils.returnConnection();
            }
        }).print();
    }



    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<String>();
        info1.add("曼联");
        info1.add("曼城");

        List<String> info2 = new ArrayList<String>();
        info2.add("3");
        info2.add("1");
        info2.add("0");

        DataSource<String> data1=env.fromCollection(info1);
        DataSource<String> data2=env.fromCollection(info2);

        data1.cross(data2).print();
    }

    public static void outJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(5, "猪头户"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(4, "杭州"));

        DataSource<Tuple2<Integer,String>> data1=env.fromCollection(info1);
        DataSource<Tuple2<Integer,String>> data2=env.fromCollection(info2);

//        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
//            @Override
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(second==null){
//                    return new Tuple3<Integer, String, String>(first.f0,first.f1,"-");
//                }else{
//                    return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
//                }
//
//        }
//        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
//            @Override
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(first==null){
//                    return new Tuple3<Integer, String, String>(second.f0,"-",second.f1);
//                }else{
//                    return new Tuple3<Integer, String, String>(second.f0,first.f1,second.f1);
//                }
//            }
//        }).print();
        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(first==null){
                    return new Tuple3<Integer, String, String>(second.f0,"-",second.f1);
                }else if(second==null){
                    return new Tuple3<Integer, String, String>(first.f0,first.f1,"-");
                }else{
                    return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                }
            }
        }).print();
    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(5, "猪头户"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(4, "杭州"));

        DataSource<Tuple2<Integer,String>> data1=env.fromCollection(info1);
        DataSource<Tuple2<Integer,String>> data2=env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
            }
        }).print();
    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data=env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                //分割每个单词
                String[] splits=input.split(",");
                for(String split:splits){
                    collector.collect(split);
                }
            }
        }).distinct().print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data=env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                //分割每个单词
                String[] splits=input.split(",");
                for(String split:splits){
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                //每个单词附上一个1
                return new Tuple2<String, Integer>(s,1);
            }
        }).groupBy(0).sum(1).print();   // grouBy  求和
    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> info=new ArrayList<Tuple2<Integer,String>>();
        info.add(new Tuple2(1,"Hadoop"));
        info.add(new Tuple2(1,"Spark"));
        info.add(new Tuple2(1,"Flink"));
        info.add(new Tuple2(2,"Java"));
        info.add(new Tuple2(2,"SpringBoot"));
        info.add(new Tuple2(3,"Linux"));
        info.add(new Tuple2(4,"Flutter"));

        DataSource<Tuple2<Integer,String>> data= env.fromCollection(info);
        data.first(3).print();                              //就是打印前三条
        System.out.println("-----华丽的分割线-----------");
        data.groupBy(0).first(2).print();           //按照第一个字段分组，每组取两条
        System.out.println("-----华丽的分割线-----------");
        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();    //按照第一个字段分组，组里面进行排序，每组取两条
    }

    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception{
        List<String> list=new ArrayList<String>();
        for(int i=1;i<=10;i++){
            list.add("Student"+i);
        }
        DataSource<String> data=env.fromCollection(list);

//        data.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                String connection=DBUtils.getConection();
//                System.out.println("connection="+connection);
//                DBUtils.returnConnection();
//                return s;
//            }
//        }).print();
        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                String connection=DBUtils.getConection();
                System.out.println("connection="+connection);
                DBUtils.returnConnection();
            }
        }).print();

    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception{
        List<Integer> list=new ArrayList<Integer>();
        for(int i=1;i<=10;i++){
            list.add(i);
        }
        DataSource<Integer> data=env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input+1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {
                return input>5;
            }
        }).print();
    }


    public static void mapFunction(ExecutionEnvironment env)throws Exception{
        List<Integer> list=new ArrayList<Integer>();
        for(int i=1;i<=10;i++){
            list.add(i);
        }
        DataSource<Integer> data=env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input+1;
            }
        }).print();
    }


}
