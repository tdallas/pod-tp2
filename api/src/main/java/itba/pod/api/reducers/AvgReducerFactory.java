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
        private int count;

        @Override
        public void beginReduce() {
            sum = 0;
            count = 0;
        }

        @Override
        public void reduce(Double diameter) {
            sum += diameter;
            count++;
        }

        @Override
        public Double finalizeReduce() {
            return count == 0 ? 0.0 : sum/count;
        }
    }
}
