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
import itba.pod.client.exceptions.QueryException;
import itba.pod.client.utils.ArgumentValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class StreetWithMaxTrees extends Query {
    public static final int QUERY_2 = 2;
    private Integer minTrees;

    private static final Logger LOGGER = LoggerFactory.getLogger(StreetWithMaxTrees.class);

    public static void main(String[] args) {
        try {
            new StreetWithMaxTrees().query();
        } catch (QueryException e) {
            e.dealWithSpecificException(LOGGER);
        }
    }

    public void query() throws QueryException {
        try {
            setup(QUERY_2);
            readAdditionalArguments();

            List<Tree> trees = readTrees();
            IList<PairNeighbourhoodStreet> streetAndNeighbourhood = super.hz.getList("g9dataSource");
            trees.forEach(t -> streetAndNeighbourhood.add(new PairNeighbourhoodStreet(t.getStreet(), t.getNeighbourhood())));

            super.fileWriter.timestampBeginMapReduce();
            List<Map.Entry<PairNeighbourhoodStreet, Long>> result = mapReduce(streetAndNeighbourhood, this.minTrees);
            super.fileWriter.timestampEndMapReduce();

            if (result.isEmpty()) {
                super.printEmptyQueryResult(QUERY_2);
            } else {
                Map<PairNeighbourhoodStreet, Long> filteredResult = filterResult(result);
                super.fileWriter.writeStreetWithMaxTrees(filteredResult);
                super.printFinishedQuery(QUERY_2);
            }

            super.hz.shutdown();
        }  catch (InvalidArgumentException | IOException | ExecutionException | InterruptedException e) {
            throw new QueryException(e);
        }
    }

    public Map<PairNeighbourhoodStreet, Long> filterResult(List<Map.Entry<PairNeighbourhoodStreet, Long>> result) {
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

        return filteredResult.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue, LinkedHashMap::new));
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
