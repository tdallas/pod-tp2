package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.combiners.CounterCombinerFactory;
import itba.pod.api.mappers.CounterMapper;
import itba.pod.api.model.Neighbourhood;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.CounterReducerFactory;
import itba.pod.client.exceptions.InvalidArgumentException;
import itba.pod.client.exceptions.QueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class TreesPerCapita extends Query {
    public static final int QUERY_1 = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(StreetWithMaxTrees.class);

    public static void main(String[] args) {
        try {
            new TreesPerCapita().query();
        } catch (QueryException e) {
            e.dealWithSpecificException(LOGGER);
        }
    }

    public void query() throws QueryException {
        try {
            setup(QUERY_1);

            Map<String, Neighbourhood> neighbourhoods = readNeighbourhoods();
            List<Tree> trees = readTrees();
            IList<String> neighbourhoodsWithTrees = super.hz.getList("g9dataSource");
            trees.forEach(t -> {
                if (neighbourhoods.containsKey(t.getNeighbourhood())) neighbourhoodsWithTrees.add(t.getNeighbourhood());
            });

            super.fileWriter.timestampBeginMapReduce();
            Map<String, Long> result = mapReduce(neighbourhoodsWithTrees);
            super.fileWriter.timestampEndMapReduce();

            if (result.isEmpty()) {
                super.printEmptyQueryResult(QUERY_1);
            } else {
                Stream<Map.Entry<String, Double>> sortedResult = filterResult(result, neighbourhoods);
                super.fileWriter.writeTreesPerCapita(sortedResult);
                super.printFinishedQuery(QUERY_1);
            }

            super.hz.shutdown();
        } catch (InvalidArgumentException | IOException | ExecutionException | InterruptedException e) {
            throw new QueryException(e);
        }
    }

    public Map<String, Long> mapReduce(IList<String> neighbourhoodsWithTrees)
            throws ExecutionException, InterruptedException {

        JobTracker jobTracker = super.hz.getJobTracker("g9treesPerPop");
        final KeyValueSource<String,String> source = KeyValueSource.fromList(neighbourhoodsWithTrees);
        Job<String,String> job = jobTracker.newJob(source);

        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new CounterMapper<>())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new CounterReducerFactory<>())
                .submit();

        return future.get();
    }

    public Stream<Map.Entry<String, Double>> filterResult(Map<String, Long> result,
                                                          Map<String, Neighbourhood> neighbourhoods) {
        Map<String, Double> calculatedResult = new LinkedHashMap<>();

        for (Map.Entry<String, Long> entry : result.entrySet()) {
            Double population = neighbourhoods.get(entry.getKey()).getPopulation().doubleValue();
            //Round to 2 decimals here
            calculatedResult.put(entry.getKey(), Math.round(((entry.getValue()/population)*100.0))/100.0);
        }

        return calculatedResult.entrySet().stream()
                .sorted(Comparator.comparingDouble(Map.Entry<String, Double>::getValue).reversed()
                        .thenComparing(Map.Entry::getKey));
    }
}
