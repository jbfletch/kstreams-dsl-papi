
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

class FunctionalPapi<K,V,VOut> implements ProcessorSupplier<K,V,K,V> {

    public interface FunctionApply<K,V,VOut> {
        VOut apply(final VOut value);
    }

    private FunctionApply<K,V,VOut> functionApply;

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
            final VOut newValue = functionApply.apply((VOut) record.value());
            context().forward(record.withValue(newValue));
            return;

        }
    }
}
