package itba;

import com.hazelcast.core.IList;
import itba.pod.api.model.Tree;
import itba.pod.client.queries.TopSpeciesWithMaxDiam;
import itba.pod.client.utils.HazelCast;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class TopSpeciesWithMaxDiamTest {
    HazelCast hz;
    IList<Tree> trees;

    @Before
    public void createTrees() {
        List<String> addresses = new LinkedList<>();

        addresses.add("127.0.0.1");

        hz = new HazelCast(addresses);
        trees = hz.getList("g9topNSpecies");
    }

    @Test
    public void testAlphabeticalOrder() throws ExecutionException, InterruptedException {
        double diameter = 60.0;
        final List<String> orderedNames = Arrays.asList("Abedul", "Jacarandá", "Lapacho", "Laurel");

        // Keep the the "unordered order", it's what's being tested
        trees.add(new Tree("", "", orderedNames.get(3), diameter));
        trees.add(new Tree("", "", orderedNames.get(1), diameter));
        trees.add(new Tree("", "", orderedNames.get(0), diameter));
        trees.add(new Tree("", "", orderedNames.get(2), diameter));

        final List<Map.Entry<String, Double>> queryResult = TopSpeciesWithMaxDiam.query(hz, trees, trees.size());

        assertArrayEquals(orderedNames.toArray(), queryResult.stream().map(Map.Entry::getKey).toArray());
    }

    @Test
    public void testDiameterDescendingOrder() throws ExecutionException, InterruptedException {
        final Double[] orderedDiameters = { 80.0, 70.0, 60.0 };

        trees.add(new Tree("", "", "Sauce", orderedDiameters[0]));
        trees.add(new Tree("", "", "Jacarandá", orderedDiameters[1]));
        trees.add(new Tree("", "", "Abedul", orderedDiameters[2]));

        final List<Map.Entry<String, Double>> queryResult = TopSpeciesWithMaxDiam.query(hz, trees, trees.size());

        assertArrayEquals(orderedDiameters, queryResult.stream().map(Map.Entry::getValue).toArray());
    }

    @Test
    public void testTopNTrees() throws ExecutionException, InterruptedException {
        trees.add(new Tree("", "", "Abedul", 80.0));
        trees.add(new Tree("", "", "Jacarandá", 70.0));
        trees.add(new Tree("", "", "Lapacho", 60.0));

        int N = trees.size() - 1;

        final List<Map.Entry<String, Double>> queryResult = TopSpeciesWithMaxDiam.query(hz, trees, N);
        final var topNTrees = queryResult.stream()
                .map(e -> new Tree("", "", e.getKey(), e.getValue()))
                .toArray();

        assertArrayEquals(topNTrees, trees.subList(0, N).toArray());
    }
}
