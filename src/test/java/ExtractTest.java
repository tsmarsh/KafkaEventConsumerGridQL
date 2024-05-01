import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

public class ExtractTest {

    private KafkaContainer kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0")).withKraft();
        kafka.start();

        System.out.println("Servers: " + kafka.getBootstrapServers());

    }

    @AfterEach
    void tearDown() {
        kafka.stop();
    }

    @Test
    void shouldDoNothing() {
        assertTrue(true);
    }
}
