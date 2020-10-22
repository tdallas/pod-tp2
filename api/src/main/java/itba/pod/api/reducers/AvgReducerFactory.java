package itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import itba.pod.api.utils.Pair;


public class AvgReducerFactory<K> implements ReducerFactory<K, Pair<Double,Integer>, Double> {
    @Override
    public Reducer<Pair<Double,Integer>, Double> newReducer(K key) {
        return new AvgReducer();
    }

    private class AvgReducer extends Reducer<Pair<Double,Integer>, Double> {

        private  double sum;
        private  int total;

        @Override
        public void beginReduce() {
            sum = 0;
            total=0;
        }

        @Override
        public void reduce(Pair<Double,Integer> p) {
            sum += p.getA();
            total =+p.getB();
        }

        @Override
        public Double finalizeReduce() {
            if(total==0){
                return 0.0;
            }
            return sum/total ;
        }
    }
}
