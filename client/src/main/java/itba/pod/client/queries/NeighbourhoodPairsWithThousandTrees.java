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
import itba.pod.client.exceptions.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class NeighbourhoodPairsWithThousandTrees extends Query {
    public static final int QUERY_5 = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(NeighbourhoodPairsWithThousandTrees.class);

    public static void main(String[] args) {
        try {
            new NeighbourhoodPairsWithThousandTrees().query();
        } catch (QueryException e) {
            e.dealWithSpecificException(LOGGER);
        }
    }

    public void query() throws QueryException {
        try {
            setup(QUERY_5);

            IList<Tree> trees = super.hz.getList("g9dataSource");
            trees.addAll(readTrees());

            super.fileWriter.timestampBeginMapReduce();
            List<Map.Entry<String, Long>> result = mapReduce(trees);
            super.fileWriter.timestampEndMapReduce();

            if (result.isEmpty()) {
                super.printEmptyQueryResult(QUERY_5);
            } else {
                List<Map.Entry<Long, SortedPair<String>>> pairedResult = pairNeighbourhoods(result);
                super.fileWriter.writeNeighbourhoodPairsWithThousandTrees(pairedResult);
                super.printFinishedQuery(QUERY_5);
            }

            super.hz.shutdown();
        }  catch (InvalidArgumentException | IOException | ExecutionException | InterruptedException e) {
            throw new QueryException(e);
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
