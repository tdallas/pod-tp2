package itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BiggerThanCollator<K extends Comparable<K>> implements Collator<Map.Entry<K, Long>, List<Map.Entry<K, Long>>> {
    private final int min;
    private final Comparator<Map.Entry<K, Long>> comparator;

    public BiggerThanCollator(int min) {
        this.min = min;
        this.comparator = null;
    }

    public BiggerThanCollator(int min, Comparator<Map.Entry<K, Long>> comparator) {
        this.min = min;
        this.comparator = comparator;
    }

    @Override
    public List<Map.Entry<K, Long>> collate(Iterable<Map.Entry<K, Long>> values) {
        return StreamSupport.stream(values.spliterator(), false)
                .filter(m -> m.getValue() > min)
                .sorted(getComparator())
                .collect(Collectors.toList());
    }

    private Comparator<Map.Entry<K, Long>> getComparator() {
        if (this.comparator == null)
            return Map.Entry.comparingByKey();
        else
            return this.comparator;
    }
}
