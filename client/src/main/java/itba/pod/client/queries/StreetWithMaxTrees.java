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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class StreetWithMaxTrees {
    private static Logger logger = LoggerFactory.getLogger(TreesPerPopulation.class);

    public static void main(String[] args) throws InvalidArgumentException {
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        String minString = System.getProperty("min");

        ArgumentValidator.validate(addresses, city, inPath, outPath, minString);
        List<String> addressesList = Arrays.asList(addresses.split(";"));
        Integer min = Integer.valueOf(minString);

        List<Tree> trees = CSVParser.readTrees(inPath, city);

        HazelCast hz = new HazelCast(addressesList);

        IList<PairNeighbourhoodStreet> streetAndNeighbourhood = hz.getList("g9dataSource");
        assert trees != null;
        trees.forEach(t -> {
            streetAndNeighbourhood.add(new PairNeighbourhoodStreet(t.getStreet(), t.getNeighbourhood()));
        });

        //TODO tomar tiempo y logearlo en un archivo
        List<Map.Entry<PairNeighbourhoodStreet, Long>> result = null;
        try {
            result = StreetWithMaxTrees.query(hz, streetAndNeighbourhood, min);
        } catch (Exception e) {
            // TODO manejar excepcion
        }

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

        //TODO escribir filtered_result en outPath

    }

    private static List<Map.Entry<PairNeighbourhoodStreet, Long>> query(HazelCast hz, IList<PairNeighbourhoodStreet> streetAndNeighbourhood, Integer min) throws ExecutionException, InterruptedException {

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
