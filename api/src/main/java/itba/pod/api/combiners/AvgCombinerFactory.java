package itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import itba.pod.api.utils.Pair;

public class AvgCombinerFactory<K> implements CombinerFactory<K, Double, Pair<Double, Integer>> {
    @Override
    public Combiner<Double, Pair<Double, Integer>> newCombiner(K key) {
        return new AvgCombiner();
    }

    private class AvgCombiner extends Combiner<Double, Pair<Double, Integer>> {
        private double sum;
        private int total;

        @Override
        public void combine(Double value) {
            sum += value;
            total++;
        }

        @Override
        public Pair<Double, Integer> finalizeChunk() {
            return new Pair<>(sum, total);
        }

        @Override
        public void reset() {
            sum = 0.0d;
            total = 0;
        }
    }
}
