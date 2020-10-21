package com.kafka.learn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        // App Properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // 1. Stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        //2. map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(value -> value.toLowerCase())
                //3. FlatMap split values by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                //4. Select a key to apply a key
                .selectKey((key, value) -> value)
                //5. Groupby selected key
                .groupByKey()
                // 6. Aggregate here count occurrences
                .count(Named.as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        // Printed the topology
        System.out.println("##################################################################################");
        System.out.println(builder.build().describe());

        // Graceful Shutdown of the app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
