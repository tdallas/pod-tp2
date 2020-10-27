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
import itba.pod.api.reducers.CounterReducerFactory;
import itba.pod.client.exceptions.InvalidArgumentException;
import itba.pod.client.exceptions.QueryException;
import itba.pod.client.utils.ArgumentValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class NeighbourhoodPairsWithMinTrees extends Query {
    public static final int QUERY_4 = 4;
    public String species;
    public Integer minTrees;

    private static final Logger LOGGER = LoggerFactory.getLogger(NeighbourhoodPairsWithMinTrees.class);

    public static void main(String[] args) {
        try {
            new NeighbourhoodPairsWithMinTrees().query();
        } catch (QueryException e) {
            e.dealWithSpecificException(LOGGER);
        }
    }

    public void query() throws QueryException {
        try {
            setup(QUERY_4);
            readAdditionalArguments();

            IList<Tree> trees = super.hz.getList("g9dataSource");
            trees.addAll(readTrees());

            super.fileWriter.timestampBeginMapReduce();
            List<Map.Entry<String, Long>> result = mapReduce(trees, this.minTrees, this.species);
            super.fileWriter.timestampEndMapReduce();

            if (result.isEmpty()) {
                super.printEmptyQueryResult(QUERY_4);
            } else {
                List<Map.Entry<String, String>> neighbourhoodPairs = pairNeighbourhoods(result);
                super.fileWriter.writeNeighbourhoodPairsWithMinTrees(neighbourhoodPairs);
                super.printFinishedQuery(QUERY_4);
            }

            super.hz.shutdown();
        } catch (InvalidArgumentException | IOException | ExecutionException | InterruptedException e) {
            throw new QueryException(e);
        }
    }

    public List<Map.Entry<String, Long>> mapReduce(final IList<Tree> trees, Integer minTrees, String species)
            throws ExecutionException, InterruptedException {
        final JobTracker jobTracker = hz.getJobTracker("g9neighbourhoodsWithMinTrees");
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper(new NeighbourhoodCountMapper(species))
                .combiner(new CounterCombinerFactory<>())
                .reducer(new CounterReducerFactory<>())
                .submit(new BiggerThanCollator<>(minTrees - 1));    // The collator's minimum is not inclusive

        return future.get();
    }

    public List<Map.Entry<String, String>> pairNeighbourhoods(final List<Map.Entry<String, Long>> queryResult) {
        final List<String> neighbourhoods = queryResult.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        final List<Map.Entry<String, String>> neighbourhoodPairs = new LinkedList<>();

        for (int i = 0; i < neighbourhoods.size() - 1; i++)
            for (int j = i + 1; j < neighbourhoods.size(); j++)
                neighbourhoodPairs.add(Map.entry(neighbourhoods.get(i), neighbourhoods.get(j)));

        return neighbourhoodPairs;
    }

    private void readAdditionalArguments() throws InvalidArgumentException {
        final String minTreesString = System.getProperty("min");
        final String species = System.getProperty("name");

        ArgumentValidator.validateQuery4(minTreesString, species);
        setMinTrees(Integer.valueOf(minTreesString));
        setSpecies(species);
    }

    public void setMinTrees(Integer minTrees) {
        this.minTrees = minTrees;
    }

    public void setSpecies(String species) {
        this.species = species;
    }
}
