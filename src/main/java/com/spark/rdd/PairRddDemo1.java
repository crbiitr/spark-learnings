package com.spark.rdd;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 17/05/19
 */
public class PairRddDemo1 {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("pairRddDemo1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Here printing key and value using Tuple
         */
        JavaRDD<String> stringJavaRDD = sc.parallelize(inputData);
        JavaPairRDD<String, String> pairRDD = stringJavaRDD.mapToPair(value -> {
            String[] arr = value.split(":");
            return new Tuple2<>(arr[0], arr[1]);
        });
        pairRDD.collect().forEach(System.out::println);

        /**
         * Here printing count key wise
         */
        JavaPairRDD<String, Long> countRDD = stringJavaRDD.mapToPair(value -> {
            String[] arr = value.split(":");
            return new Tuple2<>(arr[0], 1L);
        });

        JavaPairRDD<String, Long> sumRdd = countRDD.reduceByKey((value1, value2) -> value1 + value2);
        sumRdd.foreach(value -> System.out.println(value));
        sumRdd.foreach(tuple -> System.out.println("\n" + tuple._1 + " exits " + tuple._2 + " times"));


        /**
         * Optimization of this above code
         */
        sc.parallelize(inputData)
                .mapToPair(value -> new Tuple2<>(value.split(":")[0], 1))
                .reduceByKey((value1, value2) -> value1 + value2)
                .foreach(tuple -> System.out.println("\n" + tuple._1 + " exist " + tuple._2 + " times"));


        /**
         * Please don't user "groupByKey" in Production, bcz it crash mostly for big data
         */
        sc.parallelize(inputData)
                .mapToPair(value -> new Tuple2<>(value.split(":")[0], 1))
                .groupByKey()
                .foreach(tuple -> System.out.println("\n" + tuple._1 + " exist " + Iterables.size(tuple._2) + " times.."));

        sc.close();
    }
}
