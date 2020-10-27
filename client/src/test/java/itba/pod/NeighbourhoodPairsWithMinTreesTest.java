package itba.pod;

import com.hazelcast.core.IList;
import itba.pod.api.model.Tree;
import itba.pod.client.queries.NeighbourhoodPairsWithMinTrees;
import itba.pod.client.utils.HazelCast;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class NeighbourhoodPairsWithMinTreesTest {
    private final NeighbourhoodPairsWithMinTrees query4 = new NeighbourhoodPairsWithMinTrees();
    private IList<Tree> trees;

    @Before
    public void setupHazelcast() {
        List<String> addresses = List.of("127.0.0.1");
        HazelCast hz = new HazelCast(addresses);
        trees = hz.getList("g9minTrees");
        query4.setHazelcast(hz);
    }

    @Test
    public void testNeighbourhoodPairing() {
        // Keep the alphabetical order
        List<Map.Entry<String, Long>> treesPerNeighbourhood = List.of(
            Map.entry("A", 1L),
            Map.entry("B", 1L),
            Map.entry("C", 1L),
            Map.entry("D", 1L)
        );

        List<Map.Entry<String, String>> pairs = query4.pairNeighbourhoods(treesPerNeighbourhood);

        List<Map.Entry<String, String>> expected = new LinkedList<>();

        expected.add(Map.entry("A", "B"));
        expected.add(Map.entry("A", "C"));
        expected.add(Map.entry("A", "D"));
        expected.add(Map.entry("B", "C"));
        expected.add(Map.entry("B", "D"));
        expected.add(Map.entry("C", "D"));

        assertArrayEquals(expected.toArray(), pairs.toArray());
    }

    @Test
    public void testMinFilter() throws ExecutionException, InterruptedException {
        final String SPECIES = "Ficus";
        final Integer minTrees = 2;

        trees.add(new Tree("Retiro", "", SPECIES, 50.0));
        trees.add(new Tree("Retiro", "", SPECIES, 50.0));
        trees.add(new Tree("Belgrano", "", SPECIES, 50.0));
        trees.add(new Tree("Belgrano", "", SPECIES, 50.0));
        trees.add(new Tree("Palermo", "", SPECIES, 50.0));

        var queryResult = query4.mapReduce(trees, minTrees, SPECIES);

        List<Map.Entry<String, Long>> expected = new LinkedList<>();

        // Keep the alphabetical order
        expected.add(Map.entry("Belgrano", 2L));
        expected.add(Map.entry("Retiro", 2L));

        assertArrayEquals(expected.toArray(), queryResult.toArray());
    }

    @Test
    public void testSpeciesFilter() throws ExecutionException, InterruptedException {
        final String SPECIES = "Ficus";
        final Integer minTrees = 1;

        trees.add(new Tree("Retiro", "", SPECIES, 50.0));
        trees.add(new Tree("Retiro", "", SPECIES, 50.0));
        trees.add(new Tree("Belgrano", "", SPECIES, 50.0));
        trees.add(new Tree("Belgrano", "", "Abedul", 50.0));
        trees.add(new Tree("Recoleta", "", "Abedul", 50.0));

        var queryResult = query4.mapReduce(trees, minTrees, SPECIES);

        List<Map.Entry<String, Long>> expected = new LinkedList<>();

        // Keep the alphabetical order
        expected.add(Map.entry("Belgrano", 1L));
        expected.add(Map.entry("Retiro", 2L));

        assertArrayEquals(expected.toArray(), queryResult.toArray());
    }
}
