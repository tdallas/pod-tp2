package itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class ThousandReducerFactory implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String key) {
        return new ThousandReducer();
    }

    private class ThousandReducer extends Reducer<Long, Long> {

        private long trees;

        @Override
        public void beginReduce() {
            trees = 0;
        }

        @Override
        public void reduce(Long value) {
            trees += value;
        }

        @Override
        public Long finalizeReduce() {
            return (trees/1000) * 1000;
        }
    }
}
