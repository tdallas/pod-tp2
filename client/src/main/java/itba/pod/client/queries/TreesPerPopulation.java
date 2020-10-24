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
import itba.pod.client.utils.ArgumentValidator;
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import itba.pod.client.utils.OutputFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TreesPerPopulation {

    private static Logger logger = LoggerFactory.getLogger(TreesPerPopulation.class);
    
    public static void main(String[] args) throws InvalidArgumentException {
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        OutputFiles outputFiles=new OutputFiles(outPath);


        ArgumentValidator.validate(addresses, city, inPath, outPath);
        List<String> addressesList = Arrays.asList(addresses.split(";"));

        outputFiles.timeStampFile("Inicio de la lectura del archivo",1);
        Map<String, Neighbourhood> neighbourhoods = CSVParser.readNeighbourhoods(inPath, city);
        List<Tree> trees = CSVParser.readTrees(inPath, city);
        outputFiles.timeStampFile("Fin de la lectura del archivo",1);

        HazelCast hz = new HazelCast(addressesList);

        IList<String> neighbourhoodsWithTrees = hz.getList("g9dataSource");
        // TODO mimi que opinas de este stream? me genera duda que pisa el hz.getList()
        neighbourhoodsWithTrees = (IList<String>) trees.stream()
                .map(Tree::getNeighbourhood)
                .filter(neighbourhoods::containsKey)
                .collect(Collectors.toList());

        // Version con for each:
        //  trees.forEach(t -> {
        //      if (neighbourhoods.containsKey(t.getNeighbourhood())) neighbourhoodsWithTrees.add(t.getNeighbourhood());
        //  });

        outputFiles.timeStampFile("Inicio del trabajo de map/reduce",1);

        Map<String, Long> result = null;
        try {
            result = TreesPerPopulation.query(hz, neighbourhoodsWithTrees);
        } catch (Exception e) {
            // TODO manejar excepcion
        }
        outputFiles.timeStampFile("Fin del trabajo de map/reduce",1);

        Map<String, Double> calculated_result = new LinkedHashMap<>();
        assert result != null;
        for (Map.Entry<String, Long> entry : result.entrySet()) {
            Double population = neighbourhoods.get(entry.getKey()).getPopulation().doubleValue();
            calculated_result.put(entry.getKey(), entry.getValue()/population);
        }

        Stream<Map.Entry<String, Double>> sorted_result = calculated_result.entrySet().stream()
                                                        .sorted(Comparator.comparingDouble(Map.Entry<String, Double>::getValue).reversed()
                                                                          .thenComparing(Map.Entry::getKey));

        outputFiles.treesPerPopulationWritter(sorted_result);

    }

    private static Map<String, Long> query(HazelCast hz, IList<String> neighbourhoodsWithTrees) throws ExecutionException, InterruptedException {

        JobTracker jobTracker = hz.getJobTracker("g9treesPerPop");
        final KeyValueSource<String,String> source = KeyValueSource.fromList(neighbourhoodsWithTrees);
        Job<String,String> job = jobTracker.newJob(source);

        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new CounterMapper<>())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new CounterReducerFactory<>())
                .submit();

        return future.get();
    }
}
