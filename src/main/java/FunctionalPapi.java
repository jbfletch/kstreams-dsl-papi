import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

class FunctionalPapi<K,V> implements ProcessorSupplier<K,V,K,V> {

    public interface FunctionApply {
        Record apply(final Record value);
    }

    private FunctionApply functionApply;

    @Override
    public Processor<K, V, K, V> get() {
        return new FunctionalPapiProcessor();
    }

    public FunctionalPapi(FunctionApply functionApply) {
        this.functionApply = functionApply;
    }

    private class FunctionalPapiProcessor extends ContextualProcessor {
        @Override
        public void process(Record record) {
            context().forward(functionApply.apply(record));

            return;

        }
    }
}
