package itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OrderByCollator<K, V> implements Collator<Map.Entry<K, V>, List<Map.Entry<K, V>>> {

    private final Comparator<Map.Entry<K, V>> comparator;

    public OrderByCollator(Comparator<Map.Entry<K,V>> comparator) {
        this.comparator = comparator;
    }

    @Override
    public List<Map.Entry<K, V>> collate(Iterable<Map.Entry<K, V>> values) {
        // From https://stackoverflow.com/questions/23932061/convert-iterable-to-stream-using-java-8-jdk
        return StreamSupport.stream(values.spliterator(), false).sorted(comparator).collect(Collectors.toList());
    }
}
