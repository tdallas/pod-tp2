package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.collators.BiggerThanCollator;
import itba.pod.api.combiners.CounterCombinerFactory;
import itba.pod.api.mappers.MinTreesMapper;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.CounterReducerFactory;
import itba.pod.client.exceptions.InvalidArgumentException;
import itba.pod.client.utils.ArgumentValidator;
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import itba.pod.client.utils.OutputFiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
        List<Map.Entry<String, Long>> result = List.of();

        try {
            result = NeighbourhoodPairsWithMinTrees.query(hz, trees, minTrees, species);

            // TODO: Evaluate if this should be a custom exception
            if (result.isEmpty())
                throw new Exception();
        } catch (Exception e) {
            // TODO manejar excepcion
        }

        outputFiles.timeStampFile("Fin del trabajo de map/reduce",3);

        List<Map.Entry<String, String>> neighbourhoodPairs = NeighbourhoodPairsWithMinTrees.pairNeighbourhoods(result);
        outputFiles.writeNeighbourhoodPairsWithMinTrees(neighbourhoodPairs);
    }

    public static List<Map.Entry<String, Long>> query(HazelCast hz, final IList<Tree> trees, Integer minTrees, String species)
            throws ExecutionException, InterruptedException {

        JobTracker jobTracker = hz.getJobTracker("g9neighbourhoodsWithMinTrees");
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(trees);
        Job<String, Tree> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper(new MinTreesMapper(species))
                .combiner(new CounterCombinerFactory<>())
                .reducer(new CounterReducerFactory<>())
                .submit(new BiggerThanCollator<>(minTrees - 1));

        return future.get();
    }

    public static List<Map.Entry<String, String>> pairNeighbourhoods(final List<Map.Entry<String, Long>> queryResult) {
        final List<String> neighbourhoods = queryResult.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        List<Map.Entry<String, String>> neighbourhoodPairs = new LinkedList<>();

        for (int i = 0; i < neighbourhoods.size() - 1; i++)
            for (int j = i + 1; j < neighbourhoods.size(); j++)
                neighbourhoodPairs.add(Map.entry(neighbourhoods.get(i), neighbourhoods.get(j)));

        return neighbourhoodPairs;
    }
}
