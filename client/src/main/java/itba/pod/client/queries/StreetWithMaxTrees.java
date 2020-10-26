package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.collators.BiggerThanCollator;
import itba.pod.api.combiners.CounterCombinerFactory;
import itba.pod.api.mappers.CounterMapper;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.CounterReducerFactory;
import itba.pod.api.utils.PairNeighbourhoodStreet;
import itba.pod.client.exceptions.InvalidArgumentException;
import itba.pod.client.utils.ArgumentValidator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class StreetWithMaxTrees extends Query {
    private static final int QUERY_2 = 2;
    private Integer minTrees;

    public static void main(String[] args) throws InvalidArgumentException, IOException {
        new StreetWithMaxTrees().query();
    }

    public void query() throws InvalidArgumentException, IOException {
        setup(QUERY_2);
        readAdditionalArguments();

        List<Tree> trees = readTrees();
        IList<PairNeighbourhoodStreet> streetAndNeighbourhood = super.hz.getList("g9dataSource");
        trees.forEach(t -> streetAndNeighbourhood.add(new PairNeighbourhoodStreet(t.getStreet(), t.getNeighbourhood())));

        List<Map.Entry<PairNeighbourhoodStreet, Long>> result = List.of();
        super.fileWriter.timestampBeginMapReduce();
        try {
            result = mapReduce(streetAndNeighbourhood, this.minTrees);
        } catch (Exception e) {
            // TODO manejar excepcion
        }
        super.fileWriter.timestampEndMapReduce();

        Map<PairNeighbourhoodStreet, Long> filteredResult = filteredResult(result);
        super.fileWriter.writeStreetWithMaxTrees(filteredResult);
    }

    public Map<PairNeighbourhoodStreet, Long> filteredResult(List<Map.Entry<PairNeighbourhoodStreet, Long>> result) {
        Map<PairNeighbourhoodStreet, Long> filteredResult = new HashMap<>();
        PairNeighbourhoodStreet prevPair = null;
        PairNeighbourhoodStreet maxPair = null;
        Long count = Long.MIN_VALUE;
        assert result != null;

        for (Map.Entry<PairNeighbourhoodStreet, Long> entry : result) {
            PairNeighbourhoodStreet currPair = entry.getKey();

            if (prevPair != null && !prevPair.getNeighbourhood().equals(currPair.getNeighbourhood())) {
                filteredResult.put(maxPair, count);
                maxPair = null;
                count = Long.MIN_VALUE;
            }
            if (entry.getValue() > count) {
                maxPair = currPair.clone();
                count = entry.getValue();
            }

            prevPair = currPair.clone();
        }
        if (result.size() > 0) {
            filteredResult.put(maxPair, count);
        }

        return filteredResult;
    }

    public List<Map.Entry<PairNeighbourhoodStreet, Long>> mapReduce(IList<PairNeighbourhoodStreet> streetAndNeighbourhood,
                                                                    Integer minTrees)
            throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hz.getJobTracker("g9streetMaxTrees");
        final KeyValueSource<String, PairNeighbourhoodStreet> source = KeyValueSource.fromList(streetAndNeighbourhood);
        Job<String, PairNeighbourhoodStreet> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<PairNeighbourhoodStreet, Long>>> future = job
                .mapper(new CounterMapper<>())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new CounterReducerFactory<>())
                .submit(new BiggerThanCollator<>(minTrees));

        return future.get();
    }

    private void readAdditionalArguments() throws InvalidArgumentException {
        String minTreesString = System.getProperty("min");

        ArgumentValidator.validateQuery2(minTreesString);
        setMinTrees(Integer.valueOf(minTreesString));
    }

    public void setMinTrees(Integer minTrees) {
        this.minTrees = minTrees;
    }
}
