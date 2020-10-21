package com.kafka.learn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class FavouriteColor {
    public static void main(String[] args) {
        // Configurations
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // Streams Builder
        StreamsBuilder builder = new StreamsBuilder();

        // Read the topic as KStream, since it has no key
        // Then select a key, put stream back to intermediary kafka topic
        builder.stream("favourite-color-input")
                                                     .map((key, value) ->
                                                             KeyValue.pair(
                                                                     value.toString().split(",")[0].toLowerCase(),
                                                                     value.toString().split(",")[1].toLowerCase()
                                                             )
                                                     ).filter((key, value) -> Arrays.asList("green", "red", "blue").contains(value))
                .to("favourite-color-intermediate");


        // Read the intermediary topic into KTable
        KTable<String, String> usersTable = builder.table("favourite-color-intermediate");
        // Perform aggregation on KTable
        KTable<String, Long> colorCounts = usersTable.groupBy((key, value) -> KeyValue.pair(value, value)).count(Named.as("Counts"));
        // Store results to kafka
        colorCounts.toStream().to("favourite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.cleanUp();
        streams.start();
        // Printed the topology
        System.out.println("##################################################################################");
        System.out.println(builder.build().describe());

        // Graceful Shutdown of the app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
