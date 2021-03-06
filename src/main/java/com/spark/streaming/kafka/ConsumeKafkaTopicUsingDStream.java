package com.spark.streaming.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 18/05/19
 */
public class ConsumeKafkaTopicUsingDStream {
    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        // Spark Configs
        SparkConf sparkConf = new SparkConf().setAppName("integrationTestingWithKafka").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // Kafka configs
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "integration.testing.with.kafka");
//        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("com.spark.test", "com.spark.test2");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        /*JavaPairDStream<Long, String> results = stream.mapToPair((item -> new Tuple2<>(item.value(), 5L)))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(item -> item.swap());*/

        /*JavaPairDStream<Long, String> results = stream.mapToPair((item -> new Tuple2<>(item.value(), 5L)))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(item -> item.swap())
                .transformToPair(rdd -> rdd.sortByKey());*/

        /*JavaPairDStream<Long, String> results = stream.mapToPair((item -> new Tuple2<>(item.value(), 5L)))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(item -> item.swap())
                .transformToPair(rdd -> rdd.sortByKey());*/




    /*    stream.foreachRDD(rdd -> {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            System.out.println("========>" + stream.filter(
                    stringStringConsumerRecord -> stringStringConsumerRecord.topic().equals("com.spark.test")
            ));
        // some time later, after outputs have completed
        ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });*/


        JavaPairDStream<String, Long> results = stream.mapToPair((item -> new Tuple2<>(item.topic(), 1L)))
                .reduceByKey((x, y) -> x + y);

        results.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
