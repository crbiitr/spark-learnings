package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 16/05/19
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("reduceDemo1").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Double> list = initializeData();
        JavaRDD<Double> doubleJavaRDD = sparkContext.parallelize(list);
        Double result = doubleJavaRDD.reduce((v1, v2) -> v1 + v2);
        System.out.println("Result => " + result);
        sparkContext.close();
    }

    private static List<Double> initializeData() {
        List<Double> list = new ArrayList<>();
        list.add(35.5);
        list.add(12.49943);
        list.add(90.32);
        list.add(20.32);
        return list;
    }
}
