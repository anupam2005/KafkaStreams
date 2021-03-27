package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class FirstStreamsApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"FirstStreamsApp");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //STEP1: Get a stream from Kafka
        KStream<String,String> wordCountInput = builder.stream("word-count-input");


        KTable<String,Long> wordCounts = wordCountInput
         //STEP2: Change Data to lower Case
        .mapValues(text -> text.toLowerCase())

        //STEP3: FlatMap i.e. split values by space and return an array
        .flatMapValues(words -> Arrays.asList(words.split(" ")))

        //STEP4: Reassign keys so that each word has a key equal to its value
        .selectKey((ignoredKey,word) -> word)

        //STEP5: Group by
        .groupByKey()

        //Count
        .count();

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology,properties);

        // Print the topology to the console.
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);


        // Attach a shutdown handler to catch control-c and terminate the application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

    }

