package itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CounterReducerFactory<K> implements ReducerFactory<K, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(K key) {
        return new CounterReducer();
    }

    private class CounterReducer extends Reducer<Long, Long> {

        private volatile long sum;

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}
