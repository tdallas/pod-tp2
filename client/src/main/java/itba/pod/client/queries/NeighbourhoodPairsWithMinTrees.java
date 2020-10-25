package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.collators.TopNCollator;
import itba.pod.api.mappers.DiameterPerSpeciesMapper;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.AvgReducerFactory;
import itba.pod.client.exceptions.InvalidArgumentException;
import itba.pod.client.utils.ArgumentValidator;
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import itba.pod.client.utils.OutputFiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class NeighbourhoodPairsWithMinTrees {
    public static void main(String[] args) throws InvalidArgumentException, IOException {
        String addresses = System.getProperty("addresses");
        String city = System.getProperty("city");
        String inPath = System.getProperty("inPath");
        String outPath = System.getProperty("outPath");
        String minTreesString = System.getProperty("min");
        String species = System.getProperty("name");

        ArgumentValidator.validate(addresses, city, inPath, outPath, minTreesString, species);
        List<String> addressesList = Arrays.asList(addresses.split(";"));
        OutputFiles outputFiles = new OutputFiles(outPath);
        Integer minTrees = Integer.valueOf(minTreesString);
        HazelCast hz = new HazelCast(addressesList);
        IList<Tree> trees = hz.getList("g9dataSource");

        outputFiles.timeStampFile("Inicio de la lectura del archivo",3);
        CSVParser parser = new CSVParser();
        trees.addAll(parser.readTrees(inPath, city));
        outputFiles.timeStampFile("Fin de la lectura del archivo",3);

        outputFiles.timeStampFile("Inicio del trabajo de map/reduce",3);
        List<Map.Entry<String, Double>> result = List.of();

        try {
            result = TopSpeciesWithMaxDiam.query(hz, trees, n);
        } catch (Exception e) {
            // TODO manejar excepcion
        }
        outputFiles.timeStampFile("Fin del trabajo de map/reduce",3);

        outputFiles.TopSpeciesWithMaxDiamWriter(result);
    }

    public static List<Map.Entry<String, Double>> query(final HazelCast hz,
                                                        final IList<Tree> trees,
                                                        final Integer n) throws ExecutionException, InterruptedException {
        final JobTracker jobTracker = hz.getJobTracker("g9topNSpecies");
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper(new DiameterPerSpeciesMapper())
                .reducer(new AvgReducerFactory())
                .submit(new TopNCollator(n));

        return future.get();
    }
}