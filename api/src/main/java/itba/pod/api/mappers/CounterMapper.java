package itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CounterMapper<K, V> implements Mapper<K, V, V, Integer> {

    @Override
    public void map(K key, V value, Context<V, Integer> context) {
        context.emit(value, 1);
    }
}
