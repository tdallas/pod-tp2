package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.combiners.CounterCombinerFactory;
import itba.pod.api.mappers.NeighbourhoodCountMapper;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.ThousandReducerFactory;
import itba.pod.api.utils.SortedPair;
import itba.pod.client.exceptions.InvalidArgumentException;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class NeighbourhoodsPairsWithThousandTrees extends Query {
    private static final int QUERY_5 = 5;

    public static void main(String[] args) throws InvalidArgumentException, IOException {
        new NeighbourhoodsPairsWithThousandTrees().query();
    }

    public void query() throws InvalidArgumentException, IOException {
        setup(QUERY_5);

        IList<Tree> trees = hz.getList("g9dataSource");
        trees.addAll(readTrees());

        fileWriter.timestampBeginMapReduce();
        Map<String, Long> result = Map.of();
        try {
            result = mapReduce(trees);
        } catch (Exception e) {
            // TODO manejar excepcion
        }
        fileWriter.timestampEndMapReduce();

        List<Map.Entry<Long, SortedPair<String>>> pairedResult = pairResult(result);
        fileWriter.writeNeighbourhoodPairsWithThousandTrees(pairedResult);
    }

    private Map<String, Long> mapReduce(IList<Tree> trees) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hz.getJobTracker("g9neighbourhoodsPairs");
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(trees);
        Job<String, Tree> job = jobTracker.newJob(source);

        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new NeighbourhoodCountMapper())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new ThousandReducerFactory())
                .submit();

        return future.get();
    }

    private List<Map.Entry<Long, SortedPair<String>>> pairResult(Map<String, Long> result) {
        List<Map.Entry<String, Long>> sorted_result = result.entrySet().stream()
                                                            .filter(e -> e.getValue() >= 1000)
                                                            .sorted(Comparator
                                                                    .comparingLong(Map.Entry<String, Long>::getValue)
                                                                    .reversed()
                                                                    .thenComparing(Map.Entry::getKey))
                                                            .collect(Collectors.toList());

        List<Map.Entry<Long, SortedPair<String>>> paired_result = new LinkedList<>();

        for (int i=0; i<sorted_result.size(); i++) {
            for (int j=i+1; j<sorted_result.size() && sorted_result.get(i).getValue().equals(sorted_result.get(j).getValue()); j++) {
                paired_result.add(new AbstractMap.SimpleEntry<>(sorted_result.get(i).getValue(),
                        new SortedPair<>(sorted_result.get(i).getKey(), sorted_result.get(j).getKey())));
            }
        }

        return paired_result;
    }
}
