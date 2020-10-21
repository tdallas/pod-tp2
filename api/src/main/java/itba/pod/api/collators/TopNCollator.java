package itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TopNCollator<T extends Comparable<T>> implements Collator<Map.Entry<String, T>, List<Map.Entry<String, T>>> {
    private Integer n;

    public TopNCollator(Integer n) {
        this.n = n;
    }


    @Override
    public List<Map.Entry<String,T>> collate(Iterable<Map.Entry<String,T>> values){
        return StreamSupport.stream(values.spliterator(),false)
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(n)
                .collect(Collectors.toList());
    }
}
