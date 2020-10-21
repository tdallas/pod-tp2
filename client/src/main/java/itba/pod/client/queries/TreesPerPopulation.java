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
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TreesPerPopulation {

    private static Logger logger = LoggerFactory.getLogger(TreesPerPopulation.class);
    
    public static void main(String[] args) {
        List<String> addresses = Arrays.asList(System.getProperty("addresses").split(";"));
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");

        Map<String, Neighbourhood> neighbourhoods = CSVParser.readNeighbourhoods(inPath, city);
        List<Tree> trees = CSVParser.readTrees(inPath, city);

        HazelCast hz = new HazelCast(addresses);

        IList<String> neighbourhoodsWithTrees = hz.getList("allTrees");

        // TODO mimi que opinas de este stream? me genera duda que pisa el hz.getList()
        neighbourhoodsWithTrees = (IList<String>) trees.stream()
                .map(Tree::getNeighbourhood)
                .filter(t -> neighbourhoods.containsKey(t))
                .collect(Collectors.toList());

        // Version con for each:
        //  trees.forEach(t -> {
        //      if (neighbourhoods.containsKey(t.getNeighbourhood())) neighbourhoodsWithTrees.add(t.getNeighbourhood());
        //  });

        //TODO tomar tiempo y logearlo en un archivo
        List<Map.Entry<String, Long>> result = null;
        try {
            result = TreesPerPopulation.query(hz, neighbourhoodsWithTrees);
        } catch (Exception e) {
            // TODO manejar excepcion
        }

        Map<String, Double> result_percentage = new LinkedHashMap<>();

        for (Map.Entry<String, Long> entry : result) {
            Double population = neighbourhoods.get(entry.getKey()).getPopulation().doubleValue();
            result_percentage.put(entry.getKey(), entry.getValue()/population);
        }

        Stream<Map.Entry<String, Double>> sorted = result_percentage.entrySet().stream()
                                                        .sorted(Comparator.comparingDouble(Map.Entry<String, Double>::getValue).reversed()
                                                                          .thenComparing(Map.Entry::getKey));

        //TODO escribir sorted en outPath

    }

    private static List<Map.Entry<String, Long>> query(HazelCast hz, IList<String> neighbourhoodsWithTrees) throws ExecutionException, InterruptedException {

        JobTracker jobTracker = hz.getJobTracker("treesPerPop");
        final KeyValueSource<String,String> source = KeyValueSource.fromList(neighbourhoodsWithTrees);
        Job<String,String> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper(new CounterMapper<>())
                .combiner(new CounterCombinerFactory())
                .reducer(new CounterReducerFactory())
                .submit();

        return future.get();
    }
}
