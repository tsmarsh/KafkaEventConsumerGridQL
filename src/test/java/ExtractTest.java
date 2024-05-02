import com.tailoredshapes.gridql.kafka.KafkaExtractor;
import com.tailoredshapes.gridql.kafka.Repository;
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
    private static KafkaProducer<String, String> producer;

    private static Properties consumerProps;
    private static Properties producerProps;

    private void createTopic(String topicName) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

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

        kafka.setPortBindings(list("51091:9093"));
        kafka.start();

        String bootstrapServers = kafka.getBootstrapServers();

        System.out.println("Kafka Server: " + bootstrapServers);
        consumerProps = new Properties();
        consumerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "extract-test");
        consumerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        consumerProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        producerProps = new Properties();

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(producerProps);

    }

    @AfterAll
    static void afterAll() {
        kafka.stop();
    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    void shouldForwardEventsToTransformer() {
        String topic = "extract-test-topic";

        createTopic(topic);

        Repository<String, Stash> mockRepo = mock(Repository.class);
        Stash payload = stash("eggs", 4.0, "name", "chuck");
        Stash message = stash("id", "12345", "operation", "CREATE", "payload", payload);

        CountDownLatch latch = new CountDownLatch(1);

        try(KafkaExtractor extractor = new KafkaExtractor(consumerProps, topic, mockRepo)){
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
}
