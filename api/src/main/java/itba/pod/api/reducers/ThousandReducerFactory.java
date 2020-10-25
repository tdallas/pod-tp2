package itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class ThousandReducerFactory implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String key) {
        return new ThousandReducer();
    }

    private static class ThousandReducer extends Reducer<Long, Long> {

        private long trees;

        @Override
        public void reduce(Long value) {
            trees += value;
        }

        @Override
        public Long finalizeReduce() {
            return (Math.floorDiv(trees, 1000L)) * 1000;
        }

    }
}
