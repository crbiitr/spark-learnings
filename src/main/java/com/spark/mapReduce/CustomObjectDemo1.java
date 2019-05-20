package com.spark.mapReduce;

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
public class CustomObjectDemo1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("CustomObjectDemo1").setMaster("local[*]");
        JavaSparkContext javaSparkContext= new JavaSparkContext(sparkConf);
        List<Integer> list = initializeList();
        JavaRDD<Integer> numRdd = javaSparkContext.parallelize(list);
        JavaRDD<IntegerToSquareRoot> squareRootJavaRDD = numRdd.map(value->new IntegerToSquareRoot(value));
        squareRootJavaRDD.collect().forEach(System.out::println);

    }
    private static List<Integer> initializeList() {
        List<Integer> list2 = new ArrayList<>();
        list2.add(1);
        list2.add(2);
        list2.add(9);
        list2.add(4);
        list2.add(5);
        return list2;
    }
}
