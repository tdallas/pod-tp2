package itba.pod.client.queries;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import itba.pod.api.combiners.CounterCombinerFactory;
import itba.pod.api.mappers.CounterMapper;
import itba.pod.api.model.Tree;
import itba.pod.api.reducers.ThousandReducerFactory;
import itba.pod.api.utils.SortedPair;
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

public class NeighbourhoodsPairsWithThousandTrees {

    private static Logger logger = LoggerFactory.getLogger(NeighbourhoodsPairsWithThousandTrees.class);

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
        List<Tree> trees = parser.readTrees(inPath, city);
        outputFiles.timeStampFile("Fin de la lectura del archivo",1);

        HazelCast hz = new HazelCast(addressesList);

        IList<String> neighbourhoodsWithTrees = hz.getList("g9dataSource");
        neighbourhoodsWithTrees = (IList<String>) trees.stream()
                .map(Tree::getNeighbourhood)
                .collect(Collectors.toList());

        outputFiles.timeStampFile("Inicio del trabajo de map/reduce",1);

        Map<String, Long> result = null;
        try {
            result = NeighbourhoodsPairsWithThousandTrees.query(hz, neighbourhoodsWithTrees);
        } catch (Exception e) {
            // TODO manejar excepcion
        }
        outputFiles.timeStampFile("Fin del trabajo de map/reduce",1);

        List<Map.Entry<Long, SortedPair<String>>> paired_result = pairResult(result);

        outputFiles.NeighbourhoodsPairsWithThousandTreesWriter(paired_result);
    }

    public static Map<String, Long> query(HazelCast hz, IList<String> neighbourhoodsWithTrees) throws ExecutionException, InterruptedException {

        JobTracker jobTracker = hz.getJobTracker("g9neighbourhoodsPairs");
        final KeyValueSource<String,String> source = KeyValueSource.fromList(neighbourhoodsWithTrees);
        Job<String,String> job = jobTracker.newJob(source);

        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new CounterMapper<>())
                .combiner(new CounterCombinerFactory<>())
                .reducer(new ThousandReducerFactory())
                .submit();

        return future.get();
    }

    public static List<Map.Entry<Long, SortedPair<String>>> pairResult(Map<String, Long> result) {
        List<Map.Entry<String, Long>> sorted_result = result.entrySet().stream()
                                                            .filter(e -> e.getValue() >= 1000)
                                                            .sorted(Comparator.comparingLong(Map.Entry<String, Long>::getValue).reversed()
                                                                    .thenComparing(Map.Entry::getKey))
                                                            .collect(Collectors.toList());

        List<Map.Entry<Long, SortedPair<String>>> paired_result = new LinkedList<>();

        for (int i=0; i<sorted_result.size(); i++) {
            for (int j=i+1; j<sorted_result.size() && sorted_result.get(i).getValue().equals(sorted_result.get(j).getValue()); j++) {
                paired_result.add(new AbstractMap.SimpleEntry<>(sorted_result.get(i).getValue(), new SortedPair<>(sorted_result.get(i).getKey(), sorted_result.get(j).getKey())));
            }
        }

        return paired_result;
    }
}
