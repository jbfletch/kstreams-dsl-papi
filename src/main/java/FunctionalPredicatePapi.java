import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

/**
 * This code is not production level by any stretch of the human psyche, have fun!
 *
 * Ta, @jbfletch
 *
 */


public class FunctionalPredicatePapi<K, V> implements ProcessorSupplier<K,V,K,V> {
    private final Predicate<? super K, ? super V> predicate;

    public FunctionalPredicatePapi(final Predicate<?super K, ? super V> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new FunctionalPredicatePapiProcessor();
    }

    private class FunctionalPredicatePapiProcessor extends ContextualProcessor<K,V,K,V>{

        @Override
        public void process(Record<K, V> record) {
            if (predicate.test(record.key(), record.value())) {
                // use forward with child here and then break the loop
                // so that no record is going to be piped to multiple streams
                context().forward(record);
            }

        }
    }
}
