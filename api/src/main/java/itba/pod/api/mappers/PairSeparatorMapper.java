package itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import itba.pod.api.utils.Pair;

public class PairSeparatorMapper <K, A, B> implements Mapper<K, Pair<A, B>, A, B> {
    @Override
    public void map(K k, Pair<A,B> pair, Context<A, B> context) {
        context.emit(pair.getA(), pair.getB());
    }
}
