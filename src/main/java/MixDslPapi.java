import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Properties;

public class MixDslPapi {
    public static void main(String[] args) {
        final MixDslPapi mixDslPapiStream = new MixDslPapi();
        KafkaStreams streams = new KafkaStreams(mixDslPapiStream.getTopology(), mixDslPapiStream.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mixdslpapi-demo-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    Topology getTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> baseStream = builder.stream("sample-cdc-topic");

        KStream<String, String> insertOnly = baseStream
            .filterNot((k, v) -> k.equalsIgnoreCase("7"))
            .mapValues(v -> v.toUpperCase());

        insertOnly.to("upper-case-with-filter-topic");




        TopologyDescription description = builder.build().describe();
        System.out.println(description);





        builder.build().addProcessor("lowercase",new FunctionalPapi<>(r-> {
        return r.toString().toLowerCase();
        }),"KSTREAM-SOURCE-0000000000")
            .addSink("sinkTopic","lower-case-topic","lowercase");


        return builder.build();

    }
}
