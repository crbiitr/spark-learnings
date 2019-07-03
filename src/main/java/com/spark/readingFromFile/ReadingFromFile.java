package com.spark.readingFromFile;

import com.spark.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 29/05/19
 */
public class ReadingFromFile {
    public static void main(String[] args) {

//        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("flatMapsAndFilterDemo1").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> stringJavaRDD = sc.textFile("src/main/resources/subtitles/input.txt");
        JavaRDD<String> stringJavaRDD = sc.textFile("src/main/resources/subtitles/input-spring.txt");

        // Just printing for testing
        /*stringJavaRDD
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .foreach(word -> System.out.println(word));*/

        /**
         * Here counting words
         */
        JavaRDD<String> latterOnlyRdd = stringJavaRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]","").toLowerCase().trim());
        JavaRDD<String> justWords = latterOnlyRdd.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> removeBlankLinesRdd = justWords.filter(sentence -> StringUtils.isNotBlank(sentence));
        JavaRDD<String> justInterestingWordsOnly = removeBlankLinesRdd.filter(word -> Util.isNotBoring(word));
        JavaPairRDD<String, Long> pairRDD = justInterestingWordsOnly.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> totals = pairRDD.reduceByKey((value1, value2) -> value1 + value2);

        JavaPairRDD<Long, String> switchValues = totals.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        JavaPairRDD<Long, String> sortedPairRdd = switchValues.sortByKey(false); // Adding false for Descending order

        List<Tuple2<Long, String>> result = sortedPairRdd.take(10);
        result.forEach(System.out::println);


        sc.close();
    }
}
