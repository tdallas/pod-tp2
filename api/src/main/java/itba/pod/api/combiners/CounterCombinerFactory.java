package itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CounterCombinerFactory<K> implements CombinerFactory<K, Integer, Long> {

    @Override
    public Combiner<Integer, Long> newCombiner(K key) {
        return new CounterCombiner();
    }

    private class CounterCombiner extends Combiner<Integer, Long> {
        private long sum = 0;

        @Override
        public void combine(Integer value) {
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
