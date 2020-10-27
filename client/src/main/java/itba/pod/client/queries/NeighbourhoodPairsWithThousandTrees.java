package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.collators.BiggerThanCollator;
import itba.pod.api.combiners.CounterCombinerFactory;
import itba.pod.api.mappers.NeighbourhoodCountMapper;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.ThousandReducerFactory;
import itba.pod.api.utils.SortedPair;
import itba.pod.client.exceptions.InvalidArgumentException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class NeighbourhoodPairsWithThousandTrees extends Query {
    public static final int QUERY_5 = 5;

    public static void main(String[] args) throws InvalidArgumentException, IOException {
        new NeighbourhoodPairsWithThousandTrees().query();
    }

    public void query() throws InvalidArgumentException, IOException {
        setup(QUERY_5);

        IList<Tree> trees = super.hz.getList("g9dataSource");
        trees.addAll(readTrees());

        super.fileWriter.timestampBeginMapReduce();
        List<Map.Entry<String, Long>> result = List.of();
        try {
            result = mapReduce(trees);
        } catch (Exception e) {
            // TODO manejar excepcion
        }
        super.fileWriter.timestampEndMapReduce();

        if (result.isEmpty()) {
            List<Map.Entry<Long, SortedPair<String>>> pairedResult = pairNeighbourhoods(result);
            super.fileWriter.writeNeighbourhoodPairsWithThousandTrees(pairedResult);
            super.printFinishedQuery(QUERY_5);
        } else {
            super.printEmptyQueryResult(QUERY_5);
        }
    }

    public List<Map.Entry<String, Long>> mapReduce(IList<Tree> trees) throws ExecutionException, InterruptedException {
        final JobTracker jobTracker = super.hz.getJobTracker("g9neighbourhoodsPairs");
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper(new NeighbourhoodCountMapper())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new ThousandReducerFactory())
                .submit(new BiggerThanCollator<>(999, Comparator
                                .comparingLong(Map.Entry<String, Long>::getValue)
                                .reversed()
                                .thenComparing(Map.Entry::getKey)));

        return future.get();
    }

    public List<Map.Entry<Long, SortedPair<String>>> pairNeighbourhoods(List<Map.Entry<String, Long>> result) {
        final List<Map.Entry<Long, SortedPair<String>>> pairedNeighbourhoods = new LinkedList<>();

        for (int i = 0; i < result.size() - 1; i++) {
            for (int j = i + 1; j < result.size(); j++) {
                Map.Entry<String, Long> iResult = result.get(i);
                Map.Entry<String, Long> jResult = result.get(j);

                if (iResult.getValue().equals(jResult.getValue()))
                    pairedNeighbourhoods.add(Map.entry(iResult.getValue(),
                            new SortedPair<>(iResult.getKey(), jResult.getKey())));
            }
        }

        return pairedNeighbourhoods;
    }
}
