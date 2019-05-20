package com.spark.reduce;

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
public class ReduceDemo1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("reduceDemo1").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Double> list = initializeData();
        JavaRDD<Double> doubleJavaRDD = sparkContext.parallelize(list);
        Double result = doubleJavaRDD.reduce((v1, v2) -> v1 + v2);
        System.out.println("Results => " + result);
        sparkContext.close();
    }

    private static List<Double> initializeData() {
        List<Double> doubleArrayList = new ArrayList<>();
        doubleArrayList.add(31.5);
        doubleArrayList.add(12.49943);
        doubleArrayList.add(90.32);
        doubleArrayList.add(20.32);
        return doubleArrayList;
    }
}
