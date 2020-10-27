package itba.pod;

import com.hazelcast.core.IList;
import itba.pod.api.model.Tree;
import itba.pod.api.utils.SortedPair;
import itba.pod.client.queries.NeighbourhoodPairsWithThousandTrees;
import itba.pod.client.utils.HazelCast;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;

public class NeighbourhoodPairsWithThousandTreesTest {
    private final NeighbourhoodPairsWithThousandTrees query5 = new NeighbourhoodPairsWithThousandTrees();
    private IList<Tree> trees;

    @Before
    public void setupHazelcast() {
        List<String> addresses = List.of("127.0.0.1");
        HazelCast hz = new HazelCast(addresses);
        trees = hz.getList("g9minTrees");
        query5.setHazelcast(hz);
    }

    @Test
    public void testNeighbourhoodPairing() {
        List<Map.Entry<String, Long>> treesPerNeighbourhood = List.of(
                Map.entry("C", 2000L),
                Map.entry("D", 2000L),
                Map.entry("E", 2000L),
                Map.entry("A", 1000L),
                Map.entry("B", 1000L)
        );

        List<Map.Entry<Long, SortedPair<String>>> pairs = query5.pairNeighbourhoods(treesPerNeighbourhood);

        List<Map.Entry<Long, SortedPair<String>>> expected = List.of(
                Map.entry(2000L, new SortedPair<>("C", "D")),
                Map.entry(2000L, new SortedPair<>("C", "E")),
                Map.entry(2000L, new SortedPair<>("D", "E")),
                Map.entry(1000L, new SortedPair<>("A", "B"))
        );

        assertArrayEquals(expected.toArray(), pairs.toArray());
    }

    @Test
    public void testMapReduce() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 1000; i++)
            trees.add(new Tree("A", "", "", 0.0));

        for (int i = 0; i < 999; i++)
            trees.add(new Tree("B", "", "", 0.0));

        for (int i = 0; i < 2000; i++) {
            trees.add(new Tree("D", "", "", 0.0));
            trees.add(new Tree("C", "", "", 0.0));
        }

        List<Map.Entry<String, Long>> queryResult = query5.mapReduce(trees);

        List<Map.Entry<String, Long>> expected = List.of(
                Map.entry("C", 2000L),
                Map.entry("D", 2000L),
                Map.entry("A", 1000L)
        );

        assertArrayEquals(expected.toArray(), queryResult.toArray());
    }
}
