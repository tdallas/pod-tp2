package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.collators.TopNCollator;
import itba.pod.api.combiners.AvgCombinerFactory;
import itba.pod.api.mappers.PairSeparatorMapper;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.AvgReducerFactory;
import itba.pod.api.utils.Pair;
import itba.pod.api.utils.PairNeighbourhoodStreet;
import itba.pod.client.exceptions.InvalidArgumentException;
import itba.pod.client.utils.ArgumentValidator;
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TopSpeciesWithMaxDiam {
    private static Logger logger = LoggerFactory.getLogger(TreesPerPopulation.class);

    public static void main(String[] args) throws InvalidArgumentException {
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        String nString = System.getProperty("n");

        ArgumentValidator.validate(addresses, city, inPath, outPath, nString);
        List<String> addressesList = Arrays.asList(addresses.split(";"));
        Integer n = Integer.valueOf(nString);

        List<Tree> trees = CSVParser.readTrees(inPath, city);

        HazelCast hz = new HazelCast(addressesList);

        IList<Pair<String, Double>> speciesAndDiameter = hz.getList("g9dataSource");
        assert trees != null;
        trees.forEach(t -> {
            speciesAndDiameter.add(new Pair<>(t.getScientificName(), t.getDiameter()));
        });

        //TODO tomar tiempo y logearlo en un archivo
        List<Map.Entry<String, Double>> result = null;
        try {
            result = TopSpeciesWithMaxDiam.query(hz, speciesAndDiameter, n);
        } catch (Exception e) {
            // TODO manejar excepcion
        }

        //TODO escribir result in outPath

    }

    private static List<Map.Entry<String, Double>> query(HazelCast hz, IList<Pair<String, Double>> speciesAndDiameter, Integer n) throws ExecutionException, InterruptedException {

        JobTracker jobTracker = hz.getJobTracker("g9topNSpecies");
        final KeyValueSource<String, Pair<String,Double>> source = KeyValueSource.fromList(speciesAndDiameter);
        Job<String, Pair<String,Double>> job = jobTracker.newJob(source);

        //based on https://gist.github.com/noctarius/7784770
        ICompletableFuture<List<Map.Entry<String,Double>>> future= job
                .mapper(new PairSeparatorMapper<>())
                .combiner(new AvgCombinerFactory<>())
                .reducer(new AvgReducerFactory<>())
                .submit(new TopNCollator<>(n));

        return future.get();
    }
}
