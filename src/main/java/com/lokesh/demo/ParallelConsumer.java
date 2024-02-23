package com.lokesh.demo;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelStreamProcessor.createEosStreamProcessor;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;

public class ParallelConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelConsumer.class);

    private final ParallelStreamProcessor<String, String> parallelStreamProcessor;

    public ParallelConsumer(ParallelStreamProcessor<String, String> parallelStreamProcessor) {
        this.parallelStreamProcessor = parallelStreamProcessor;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            // Backwards compatibility, assume localhost
            LOGGER.error("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        final Properties appProperties = SimpleConsumer.loadConfig(args[0]);

        final Consumer<String, String> consumer = new KafkaConsumer<>(appProperties);

        @SuppressWarnings("rawtypes")
        final ParallelConsumerOptions options = ParallelConsumerOptions.<String, String>builder().ordering(KEY)
                .maxConcurrency(16).consumer(consumer).commitMode(PERIODIC_CONSUMER_SYNC).build();

        @SuppressWarnings("unchecked")
        ParallelStreamProcessor<String, String> eosStreamProcessor = createEosStreamProcessor(options);

        final ParallelConsumer parallelConsumer = new ParallelConsumer(eosStreamProcessor);

        Runtime.getRuntime().addShutdownHook(new Thread(parallelConsumer::shutdown));
        parallelConsumer.runConsume(appProperties);

    }

    public void runConsume(final Properties appProperties) {

        String topic = appProperties.getProperty("input.topic.name");

        LOGGER.info("Subscribing Parallel Consumer to consume from {} topic", topic);
        parallelStreamProcessor.subscribe(Collections.singletonList(topic));

        LOGGER.info("Polling for records. This method blocks", topic);
        parallelStreamProcessor
                .poll(context -> LOGGER.info("key = {}, value = {}", context.getSingleConsumerRecord().key(),
                        context.getSingleConsumerRecord().value()));
    }

    public void shutdown() {
        LOGGER.info("shutting down");
        if (parallelStreamProcessor != null) {
            parallelStreamProcessor.close();
        }
    }
}
