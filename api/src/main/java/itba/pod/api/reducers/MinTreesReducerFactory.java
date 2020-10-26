package itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Map;

public class MinTreesReducerFactory implements ReducerFactory<String, Long, Map.Entry<String, String>> {
    @Override
    public Reducer<Long, Map.Entry<String, String>> newReducer(String s) {
        return new MinTreesReducer();
    }

    private class MinTreesReducer extends Reducer<Long, Map.Entry<String, String>> {

        @Override
        public void reduce(Long treesInNeighbourhood) {

        }

        @Override
        public Map.Entry<String, String> finalizeReduce() {
            return null;
        }
    }
}
