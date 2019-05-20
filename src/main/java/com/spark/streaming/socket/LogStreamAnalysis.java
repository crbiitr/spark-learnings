package com.spark.streaming.socket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @author Chetan Raj
 * @implNote Before running this main function first run LoggingServer's main function.
 * @since : 17/05/19
 */
public class LogStreamAnalysis {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("logStreamingAnalysis");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaReceiverInputDStream<String> inputData = streamingContext.socketTextStream("localhost", 8989);
        /**
         * Adding reduceByKey WithOut Window feature
         */
        /*JavaPairDStream<String, Long> result = inputData.mapToPair(value -> new Tuple2<>(value.split(",")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2);*/


        /**
         * Adding reduceByKey with Window feature
         */
        JavaPairDStream<String, Long> result = inputData.mapToPair(value -> new Tuple2<>(value.split(",")[0], 1L))
                .reduceByKeyAndWindow((value1, value2) -> value1 + value2, Durations.minutes(1));


        // NOTE: print statement is important, bcz if there is no output than spark gives the error
        result.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
