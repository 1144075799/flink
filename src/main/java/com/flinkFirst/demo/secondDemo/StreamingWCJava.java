package com.flinkFirst.demo.secondDemo;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java api 来开发Flink实时处理应用程序
 *
 * wc统计的数据源自于socket
 */
public class StreamingWCJava {


    public static void main(String[] args) throws Exception {
        //step1：获取执行上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //step2：读取数据
        DataStreamSource text=env.socketTextStream("localhost",9999);

        //step3: transform

        //text.flatMap(new MyFlatMapFounction())//.keyBy("word")
        text.flatMap(new RichFlatMapFunction<String,WC>() {

            @Override
            public void flatMap(String value, Collector<WC> collector) throws Exception {
                String[] tokens=value.toLowerCase().split(",");
                for(String token:tokens){
                    if(token.length()>0){
                        collector.collect(new WC(token.trim(),1));
                    }
                }
            }
        })
             .keyBy(new KeySelector<WC,String>() {

                 @Override
                 public String getKey(WC wc) throws Exception {
                     return wc.word;
                 }
             }).timeWindow(Time.seconds(5)).sum("count").print().setParallelism(1);

        env.execute("StreamingWCJava");
    }

    public static class MyFlatMapFounction implements FlatMapFunction<String,WC> {

        @Override
        public void flatMap(String value, Collector<WC> collector) throws Exception {
            String[] tokens=value.toLowerCase().split(",");
            for(String token:tokens){
                if(token.length()>0){
                    collector.collect(new WC(token.trim(),1));
                }
            }
        }
    }

    public static class WC{
        private String word;
        private int count;

        public WC(){};

        public WC(String word,int count){
            this.word=word;
            this.count=count;
        }


        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }
    }

}
