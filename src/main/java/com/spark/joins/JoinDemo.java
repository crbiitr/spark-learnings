package com.spark.joins;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 01/06/19
 */
public class JoinDemo {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("RddJoin").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "Aman"));
        userRaw.add(new Tuple2<>(2, "Chaman"));
        userRaw.add(new Tuple2<>(3, "Daman"));
        userRaw.add(new Tuple2<>(4, "Chetan"));
        userRaw.add(new Tuple2<>(5, "Karan"));
        userRaw.add(new Tuple2<>(6, "Anjali"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(userRaw);

        // Inner join
        System.out.println("\n<= Inner join =>\n");
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
        joinedRdd.collect().forEach(System.out::println);

        // Left outer join
        System.out.println("\n<= Left outer join =>\n");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoinedRdd = visits.leftOuterJoin(users);
        leftJoinedRdd.collect().forEach(System.out::println);

        // Right outer join
        System.out.println("\n<= Right outer join =>\n");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoinedRdd = visits.rightOuterJoin(users);
        rightJoinedRdd.collect().forEach(System.out::println);

        // Full join
        System.out.println("\n<= Full join =>\n");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullJoinedRdd = visits.fullOuterJoin(users);
        fullJoinedRdd.collect().forEach(System.out::println);

        // Cartesian join (Cross join)
        System.out.println("\n<= Cartesian join =>\n");
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoinedRdd = visits.cartesian(users);
        cartesianJoinedRdd.collect().forEach(System.out::println);


        sc.close();
    }
}
