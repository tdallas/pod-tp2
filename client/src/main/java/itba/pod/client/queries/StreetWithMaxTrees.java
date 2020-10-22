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
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class StreetWithMaxTrees {
    private static Logger logger = LoggerFactory.getLogger(TreesPerPopulation.class);

    public static void main(String[] args) {
        List<String> addresses = Arrays.asList(System.getProperty("addresses").split(";"));
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        Integer min = Integer.valueOf(System.getProperty("min"));

        List<Tree> trees = CSVParser.readTrees(inPath, city);


        HazelCast hz = new HazelCast(addresses);
        IList<PairNeighbourhoodStreet> queryTrees = hz.getList("allTrees");
        assert trees != null;
        trees.forEach(t -> {
            queryTrees.add(new PairNeighbourhoodStreet(t.getStreet(), t.getNeighbourhood()));
        });


        //TODO tomar tiempo y logearlo en un archivo
        List<Map.Entry<PairNeighbourhoodStreet, Long>> result = null;
        try {
            result = StreetWithMaxTrees.query(hz, queryTrees, min);
        } catch (Exception e) {
            // TODO manejar excepcion
        }

        Map<PairNeighbourhoodStreet, Long> filterresult = new HashMap<>();
        PairNeighbourhoodStreet neighbourhoodprev = null;
        Long count = Long.MIN_VALUE;
        assert result != null;
        for (Map.Entry<PairNeighbourhoodStreet, Long> me : result) {
            PairNeighbourhoodStreet neighbourhoodcurr = me.getKey();

            if (neighbourhoodprev != null && !neighbourhoodprev.getNeighbourhood().equals(neighbourhoodcurr.getNeighbourhood())) {
                filterresult.put(neighbourhoodprev, count);
                count = Long.MIN_VALUE;
            } else {
                if (me.getValue() > count) {
                    count = me.getValue();
                }

            }
            //TODO chequear si no hace referencia
            neighbourhoodprev = neighbourhoodcurr.clone();

        }

        //TODO escribir filterresult en outPath


    }

    private static List<Map.Entry<PairNeighbourhoodStreet, Long>> query(HazelCast hz, IList<PairNeighbourhoodStreet> queryTrees, Integer min) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hz.getJobTracker("StreetMaxTrees");
        final KeyValueSource<String, PairNeighbourhoodStreet> source = KeyValueSource.fromList(queryTrees);
        Job<String, PairNeighbourhoodStreet> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<PairNeighbourhoodStreet, Long>>> future = job
                .mapper(new CounterMapper<>())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new CounterReducerFactory<>())
                .submit(new BiggerThanCollator<>(min));

        return future.get();
    }
}
