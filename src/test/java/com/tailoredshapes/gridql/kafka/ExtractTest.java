package com.tailoredshapes.gridql.kafka;

import com.tailoredshapes.gridql.load.Repository;
import com.tailoredshapes.stash.Stash;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.tailoredshapes.stash.Stash.stash;
import static com.tailoredshapes.underbar.ocho.UnderBar.list;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExtractTest {

    private static KafkaContainer kafka;
    private KafkaProducer<String, String> producer;

    private static Properties consumerProps;
    private static Properties producerProps;
    private static String servers;

    private void createTopic(String topicName, String servers) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);

        try (AdminClient admin = AdminClient.create(config)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic", e);
        }
    }

    @BeforeAll
    static void beforeAll() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
        kafka.start();

        servers = kafka.getBootstrapServers();

        System.out.println("Kafka Server: " + servers);
        consumerProps = new Properties();
        consumerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "extract-test");
        consumerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        consumerProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        consumerProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        producerProps = new Properties();

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    @BeforeEach
    void setUp() {
        producer = new KafkaProducer<>(producerProps);
    }

    @AfterAll
    static void afterAll() {
        kafka.stop();
    }

    @Test
    void shouldForwardCreateEventsToTransformer() {
        String topic = "extract-test-topic";

        createTopic(topic, servers);

        Properties createProps = new Properties();
        createProps.putAll(consumerProps);
        createProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "extract-create-test");

        Repository<String, Stash> mockRepo = mock(Repository.class);
        Stash payload = stash("eggs", 4.0, "name", "chuck");
        Stash message = stash("id", "12345", "operation", "CREATE", "payload", payload);

        CountDownLatch latch = new CountDownLatch(1);


        try(KafkaExtractor extractor = new KafkaExtractor(createProps, topic, mockRepo)){
            extractor.start();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.toJSONString());

            producer.send(record, ((recordMetadata, e) -> {
                System.out.println("Message sent");
                latch.countDown();
            }));

            latch.await(10, TimeUnit.SECONDS);
            System.out.println("Latch cleared");
            verify(mockRepo, Mockito.timeout(2000)).create(payload);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldForwardDeleteEventsToTransformer() {
        String topic = "extract-delete-test-topic";

        createTopic(topic, servers);

        Properties deleteProps = new Properties();
        deleteProps.putAll(consumerProps);
        deleteProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "extract-delete-test");

        Repository<String, Stash> mockRepo = mock(Repository.class);
        Stash payload = stash();
        String id = "123124";
        Stash message = stash("id", id, "operation", "DELETE", "payload", payload);

        CountDownLatch latch = new CountDownLatch(1);

        try(KafkaExtractor extractor = new KafkaExtractor(deleteProps, topic, mockRepo)){
            extractor.start();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.toJSONString());

            producer.send(record, ((recordMetadata, e) -> {
                System.out.println("Message sent");
                latch.countDown();
            }));

            latch.await(10, TimeUnit.SECONDS);
            System.out.println("Latch cleared");
            verify(mockRepo, Mockito.timeout(2000)).delete(id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void shouldForwardUpdateEventsToTransformer() {
        String topic = "extract-update-test-topic";

        createTopic(topic, servers);

        Properties updateProps = new Properties();
        updateProps.putAll(consumerProps);
        updateProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "extract-update-test");

        Repository<String, Stash> mockRepo = mock(Repository.class);
        Stash payload = stash("eggs", 8.0, "name", "chuck");
        String id = "123124";
        Stash message = stash("id", id, "operation", "UPDATE", "payload", payload);

        CountDownLatch latch = new CountDownLatch(1);

        try(KafkaExtractor extractor = new KafkaExtractor(updateProps, topic, mockRepo)){
            extractor.start();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.toJSONString());

            producer.send(record, ((recordMetadata, e) -> {
                System.out.println("Message sent");
                latch.countDown();
            }));

            latch.await(10, TimeUnit.SECONDS);
            System.out.println("Latch cleared");
            verify(mockRepo, Mockito.timeout(2000)).update(id, payload);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
