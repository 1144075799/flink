package com.flinkFirst.demo.thirdDemo;

import java.util.Random;

public class DBUtils {

    static String getConection(){
        return String.valueOf(new Random().nextInt(10));
    }

    static void returnConnection(){

    }

}
