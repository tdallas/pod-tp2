package itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;


public class AvgReducerFactory implements ReducerFactory<String, Double, Double> {
    @Override
    public Reducer<Double, Double> newReducer(String key) {
        return new AvgReducer();
    }

    private class AvgReducer extends Reducer<Double, Double> {
        private double sum;
        private long count;

        @Override
        public void reduce(Double diameter) {
            sum += diameter;
            count++;
        }

        @Override
        public Double finalizeReduce() {
            return count == 0L ? 0.0d : sum / count;
        }
    }
}
