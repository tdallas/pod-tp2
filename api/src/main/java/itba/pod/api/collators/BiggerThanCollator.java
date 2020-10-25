package itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BiggerThanCollator<K extends Comparable<K>> implements Collator<Map.Entry<K, Long>, List<Map.Entry<K, Long>>> {
    private int min;

    public BiggerThanCollator(int min) {
        this.min = min;
    }

    @Override
    public List<Map.Entry<K, Long>> collate(Iterable<Map.Entry<K, Long>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .filter(m -> m.getValue() > min)
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());
    }
}
