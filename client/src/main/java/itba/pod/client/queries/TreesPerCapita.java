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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TreesPerCapita {

    private static Logger logger = LoggerFactory.getLogger(TreesPerCapita.class);
    
    public static void main(String[] args) throws InvalidArgumentException, IOException {
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");

        ArgumentValidator.validate(addresses, city, inPath, outPath);
        List<String> addressesList = Arrays.asList(addresses.split(";"));
        OutputFiles outputFiles=new OutputFiles(outPath);

        outputFiles.timeStampFile("Inicio de la lectura del archivo",1);
        CSVParser parser = new CSVParser();
        Map<String, Neighbourhood> neighbourhoods = parser.readNeighbourhoods(inPath, city);
        List<Tree> trees = parser.readTrees(inPath, city);
        outputFiles.timeStampFile("Fin de la lectura del archivo",1);

        HazelCast hz = new HazelCast(addressesList);

        IList<String> neighbourhoodsWithTrees = hz.getList("g9dataSource");

        trees.forEach(t->{
            if(neighbourhoods.containsKey(t.getNeighbourhood())) neighbourhoodsWithTrees.add(t.getNeighbourhood());
        });

        outputFiles.timeStampFile("Inicio del trabajo de map/reduce",1);

        Map<String, Long> result = null;
        try {
            result = TreesPerCapita.query(hz, neighbourhoodsWithTrees);
        } catch (Exception e) {
            // TODO manejar excepcion
        }
        outputFiles.timeStampFile("Fin del trabajo de map/reduce",1);

        assert result != null;
        Stream<Map.Entry<String, Double>> sorted_result=filterResult(result,neighbourhoods);

        outputFiles.writeTreesPerCapita(sorted_result);

    }

    public static Map<String, Long> query(HazelCast hz, IList<String> neighbourhoodsWithTrees) throws ExecutionException, InterruptedException {

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

    public static Stream<Map.Entry<String, Double>> filterResult(Map<String, Long> result, Map<String, Neighbourhood> neighbourhoods){
        Map<String, Double> calculated_result = new LinkedHashMap<>();

        for (Map.Entry<String, Long> entry : result.entrySet()) {
            Double population = neighbourhoods.get(entry.getKey()).getPopulation().doubleValue();
            //Round to 2 decimals here
            calculated_result.put(entry.getKey(), Math.round(((entry.getValue()/population)*100.0))/100.0);
        }

        return calculated_result.entrySet().stream()
                .sorted(Comparator.comparingDouble(Map.Entry<String, Double>::getValue).reversed()
                        .thenComparing(Map.Entry::getKey));

    }
}
