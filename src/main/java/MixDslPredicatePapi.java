import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.utils.Json;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import serdes.JsonSerde;
import java.util.Properties;

/**
 * This code is not production level by any stretch of the human psyche, have fun!
 *
 * Ta, @jbfletch
 *
 */
public class MixDslPredicatePapi {
    private final Serde<JsonNode> jsonSerde = new JsonSerde();

    public static void main(String[] args) {
        final MixDslPredicatePapi mixDslPredicatePapi = new MixDslPredicatePapi();
        KafkaStreams streams = new KafkaStreams(mixDslPredicatePapi.getTopology(), mixDslPredicatePapi.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mix-dsl-predicate-papi-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, jsonSerde.getClass().getName());
        return props;
    }

    Topology getTopology(){

        Predicate<String, JsonNode> isError = (String k, JsonNode v) ->
            v.path("ERROR")
                .asText()
                .equals("YES");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,JsonNode> baseStream = builder.stream("example-input-topic", Consumed.with(Serdes.String(),jsonSerde));

        KStream<String, JsonNode> changedStream = baseStream
            .mapValues(v-> {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode node = mapper.createObjectNode();
                node.set("app",v.path("app"));
                node.set("op_type",v.path("op_type"));
                node.set("completion",v.path("completion"));
                node.put("chant","GO BILLS");
                node.put("ERROR", v.path("completion").asText().equalsIgnoreCase("Y") ? "NO" :"YES");
                return (JsonNode) node;
            })
        .filterNot(isError);

        changedStream.print(Printed.<String, JsonNode>toSysOut().withLabel("no errors"));

        changedStream.to("output-topic-with-filter", Produced.with(Serdes.String(),jsonSerde));
        TopologyDescription description = builder.build().describe();
        System.out.println(description);

        builder.build().addProcessor("errors", new FunctionalPredicatePapi<>(isError), "KSTREAM-MAPVALUES-0000000001")
            .addSink("sinkTopic", "output-topic-with-errors", "errors");

        return builder.build();

    }

}
