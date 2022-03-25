import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import serdes.JsonSerde;

import java.io.IOException;

public class MixDslPredicatePapiTest {
    private final Serde<JsonNode> jsonSerde = new JsonSerde();
    private final MixDslPredicatePapi app = new MixDslPredicatePapi();

    private final ObjectMapper mapper = new ObjectMapper()
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    @RegisterExtension
    final TestTopologyExtension<Object, JsonNode> testTopology =
        new TestTopologyExtension<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void dslTopologyTest()
        throws IOException {

        JsonNode node1 = mapper.readValue("{'app': 'go-bills', 'op_type': 'screen-pass', 'completion': 'Y'}", JsonNode.class);
        JsonNode node2 = mapper.readValue("{'app': 'go-bills', 'op_type': 'screen-pass', 'completion': 'N'}", JsonNode.class);

        this.testTopology.input()
            .add("1", node1)
            .add("2", node2);

        this.testTopology.tableOutput("output-topic-with-filter").withSerde(Serdes.String(), jsonSerde)
            .expectNextRecord().hasKey("1")
            .expectNoMoreRecord();

        this.testTopology.tableOutput("output-topic-with-errors").withSerde(Serdes.String(), jsonSerde)
            .expectNextRecord().hasKey("2")
            .expectNoMoreRecord();


    }


}
