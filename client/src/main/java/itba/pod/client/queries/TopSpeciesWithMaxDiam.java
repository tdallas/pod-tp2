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
import itba.pod.client.exceptions.QueryException;
import itba.pod.client.utils.ArgumentValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TopSpeciesWithMaxDiam extends Query {
    public static final int QUERY_3 = 3;
    private Integer n;

    private static final Logger LOGGER = LoggerFactory.getLogger(TopSpeciesWithMaxDiam.class);

    public static void main(String[] args) {
        try {
            new TopSpeciesWithMaxDiam().query();
        } catch (QueryException e) {
            e.dealWithSpecificException(LOGGER);
        }
    }

    public void query() throws QueryException {
        try {
            setup(QUERY_3);
            readAdditionalArguments();

            IList<Tree> trees = super.hz.getList("g9dataSource");
            trees.addAll(readTrees());

            super.fileWriter.timestampBeginMapReduce();
            List<Map.Entry<String, Double>> result = mapReduce(trees, this.n);
            super.fileWriter.timestampEndMapReduce();

            if (result.isEmpty()) {
                super.printEmptyQueryResult(QUERY_3);
            } else {
                super.fileWriter.writeTopSpeciesWithMaxDiam(result);
                super.printFinishedQuery(QUERY_3);
            }

            super.hz.shutdown();
        }  catch (InvalidArgumentException | IOException | ExecutionException | InterruptedException e) {
            throw new QueryException(e);
        }
    }

    public List<Map.Entry<String, Double>> mapReduce(final IList<Tree> trees, final Integer n)
            throws ExecutionException, InterruptedException {
        final JobTracker jobTracker = super.hz.getJobTracker("g9topNSpecies");
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper(new DiameterPerSpeciesMapper())
                .reducer(new AvgReducerFactory())
                .submit(new TopNCollator(n));

        return future.get();
    }

    private void readAdditionalArguments() throws InvalidArgumentException {
        String nString = System.getProperty("n");

        ArgumentValidator.validateQuery3(nString);
        setN(Integer.valueOf(nString));
    }

    public void setN(Integer n) {
        this.n = n;
    }
}
