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
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import itba.pod.client.utils.OutputFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class StreetWithMaxTrees {
    private static Logger logger = LoggerFactory.getLogger(TreesPerPopulation.class);

    public static void main(String[] args) throws InvalidArgumentException, IOException {
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        String minString = System.getProperty("min");

        ArgumentValidator.validate(addresses, city, inPath, outPath, minString);
        List<String> addressesList = Arrays.asList(addresses.split(";"));
        OutputFiles outputFiles=new OutputFiles(outPath);
        Integer min = Integer.valueOf(minString);

        outputFiles.timeStampFile("Inicio de la lectura del archivo",2);
        CSVParser parser = new CSVParser();
        List<Tree> trees = parser.readTrees(inPath, city);
        outputFiles.timeStampFile("Fin de la lectura del archivo",2);

        HazelCast hz = new HazelCast(addressesList);
        IList<PairNeighbourhoodStreet> streetAndNeighbourhood = hz.getList("g9dataSource");

        assert trees != null;
        trees.forEach(t -> {
            streetAndNeighbourhood.add(new PairNeighbourhoodStreet(t.getStreet(), t.getNeighbourhood()));
        });

        outputFiles.timeStampFile("Inicio del trabajo de map/reduce",2);
        List<Map.Entry<PairNeighbourhoodStreet, Long>> result = null;
        try {
            result = StreetWithMaxTrees.query(hz, streetAndNeighbourhood, min);

        } catch (Exception e) {
            // TODO manejar excepcion
        }
        outputFiles.timeStampFile("Fin del trabajo de map/reduce",2);

        assert result != null;
        Map<PairNeighbourhoodStreet,Long> filtered_result=filtered_result(result);

        outputFiles.StreetWithMaxTreesWriter(filtered_result);
    }

    public static Map<PairNeighbourhoodStreet,Long> filtered_result(List<Map.Entry<PairNeighbourhoodStreet, Long>> result){
        Map<PairNeighbourhoodStreet, Long> filtered_result = new HashMap<>();
        PairNeighbourhoodStreet pairPrev = null;
        PairNeighbourhoodStreet pairMax = null;
        Long count = Long.MIN_VALUE;
        assert result != null;
        for (Map.Entry<PairNeighbourhoodStreet, Long> entry : result) {
            PairNeighbourhoodStreet pairCurr = entry.getKey();

            if (pairPrev != null && !pairPrev.getNeighbourhood().equals(pairCurr.getNeighbourhood())) {
                filtered_result.put(pairMax, count);
                pairMax = null;
                count = Long.MIN_VALUE;
            } else {
                if (entry.getValue() > count) {
                    pairMax = pairCurr.clone();
                    count = entry.getValue();
                }
            }
            pairPrev = pairCurr.clone();
        }
        return filtered_result;
    }
    public static List<Map.Entry<PairNeighbourhoodStreet, Long>> query(HazelCast hz, IList<PairNeighbourhoodStreet> streetAndNeighbourhood, Integer min) throws ExecutionException, InterruptedException {

        JobTracker jobTracker = hz.getJobTracker("g9streetMaxTrees");
        final KeyValueSource<String, PairNeighbourhoodStreet> source = KeyValueSource.fromList(streetAndNeighbourhood);
        Job<String, PairNeighbourhoodStreet> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<PairNeighbourhoodStreet, Long>>> future = job
                .mapper(new CounterMapper<>())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new CounterReducerFactory<>())
                .submit(new BiggerThanCollator<>(min));

        return future.get();
    }
}
