import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MixDslPapiTest {
    private final MixDslPapi app = new MixDslPapi();

    @RegisterExtension
    final TestTopologyExtension<Object,String> testTopology =
        new TestTopologyExtension<Object, String>(this.app.getTopology(),this.app.getKafkaProperties());

    @Test
    void uppercaseFilterTest(){
        this.testTopology.input()
            .add("1", "Hello")
            .add("2","Angela Lansbury")
            .add("7","not on my filters watch");

        this.testTopology.tableOutput("upper-case-with-filter-topic").withSerde(Serdes.String(), Serdes.String())
            .expectNextRecord().hasKey("1").hasValue("HELLO")
            .expectNextRecord().hasKey("2").hasValue("ANGELA LANSBURY")
            .expectNoMoreRecord();
        this.testTopology.tableOutput("lower-case-topic").withSerde(Serdes.String(), Serdes.String())
            .expectNextRecord().hasKey("1").hasValue("hello")
            .expectNextRecord().hasKey("2").hasValue("angela lansbury")
            .expectNextRecord().hasKey("7").hasValue("not on my filters watch")
            .expectNoMoreRecord();
    }
}
