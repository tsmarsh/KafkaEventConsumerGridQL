package com.tailoredshapes.gridql.kafka;

import com.tailoredshapes.stash.Stash;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.logging.Logger;

enum Operations {
    CREATE,
    DELETE,
    UPDATE
}

public class KafkaExtractor implements AutoCloseable {

    private final Logger logger = Logger.getLogger(KafkaExtractor.class.getName());
    private final Repository<String, Stash> repo;
    private KafkaStreams streams;

    public KafkaExtractor(Properties props, String topic, Repository<String, Stash> repo) {
        this.repo = repo;

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String>  kStream = builder.stream(topic);
        kStream.foreach(this::forward);

        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private void forward(String key, String value) {
        System.out.println("Processing...");
        Stash message = Stash.parseJSON(value);
        String operation = message.asString("operation");

        try {
            Operations operations = Operations.valueOf(operation);
            switch (operations) {
                case CREATE:
                    this.repo.create(message.asStash("payload"));
                    break;
                case DELETE:
                    this.repo.delete(message.asString("id"));
                    break;
                case UPDATE:
                    this.repo.update(message.asString("id"), message.asStash("payload"));
                    break;
            }
        } catch (IllegalArgumentException iae) {
            logger.severe(() -> "Unknown operation: " + operation);
            //should put message on DLQ
        }
    }

    @Override
    public void close() throws Exception {
        streams.close();
    }
}
