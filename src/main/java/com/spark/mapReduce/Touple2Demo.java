package com.spark.mapReduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 17/05/19
 */
public class Touple2Demo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ToupleDemo1").setMaster("local[*]");
        JavaSparkContext javaSparkContext= new JavaSparkContext(sparkConf);
        List<Integer> list = initializeList();
        JavaRDD<Integer> numRdd = javaSparkContext.parallelize(list);
        JavaRDD<IntegerToSquareRoot> squareRootJavaRDD = numRdd.map(value->new IntegerToSquareRoot(value));

        // JavaRDD<Tuple2> tuple2JavaRDD = numRdd.map(value -> new Tuple2(value,Math.sqrt(value))); // It will also work
        JavaRDD<Tuple2<Integer,Double>> tuple2JavaRDD = numRdd.map(value -> new Tuple2(value,Math.sqrt(value)));

        tuple2JavaRDD.collect().forEach(System.out::println);

    }
    private static List<Integer> initializeList() {
        List<Integer> list2 = new ArrayList<>();
        list2.add(1);
        list2.add(2);
        list2.add(9);
        list2.add(9);
        list2.add(5);
        return list2;
    }
}
