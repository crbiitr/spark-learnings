package com.spark.flatMapsAndFilter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 17/05/19
 */
public class FlatMapAndFilterDemo1 {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("flatMapsAndFilterDemo1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Applying FlatMap which take input as 1 or more and return result one or more
         */
        JavaRDD<String> stringJavaRDD = sc.parallelize(inputData);
        JavaRDD<String> words = stringJavaRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        words.foreach(word -> System.out.println(word));

        /**
         * Applying filter on word RDD, return true iff length of word is > 1
         */
        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1);
        filteredWords.foreach(word -> System.out.println(word));


        /**
         * Refactoring above code in Single line
         */
        sc.parallelize(inputData)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> word.length() > 1)
                .foreach(word -> System.out.println(word));

        sc.close();
    }
}
