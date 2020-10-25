package itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CounterCombinerFactory<K> implements CombinerFactory<K, Long, Long> {

    @Override
    public Combiner<Long, Long> newCombiner(K key) {
        return new CounterCombiner();
    }

    private class CounterCombiner extends Combiner<Long, Long> {
        private long sum = 0;

        @Override
        public void combine(Long value) {
            sum++;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }
}
