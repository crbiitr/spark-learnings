package com.spark.map;

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
public class MapDemo1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapDemo1").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> list = initializeList();
        JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(list);

        JavaRDD<Double> sqrtRdd = integerJavaRDD.map(value -> Math.sqrt(value));

        // Serialization exception
        // doubleJavaRDD.foreach(System.out::println);

        sqrtRdd.collect().forEach(System.out::println);

        sqrtRdd.foreach(value -> System.out.println(value));

        // How many elements in sqrtRdd using JavaRDD.count() method
        System.out.println(sqrtRdd.count());

        // Case: 1 Count using just map and reduce
        int count = sqrtRdd.map(value -> 1).reduce((v1, v2) -> v1 + v2);
        System.out.println("Way 1: Count using map and reduce ==> " + count);


        // CASE: 2 Count using just map and reduce
        JavaRDD<Long> countRdd= sqrtRdd.map(value -> 1L);
        Long count1 = countRdd.reduce((v1, v2) -> v1 + v2);
        System.out.println("Way 2: Count using map and reduce ==> " + count1);

        javaSparkContext.close();
    }

    private static List<Integer> initializeList() {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        return list;
    }

}
