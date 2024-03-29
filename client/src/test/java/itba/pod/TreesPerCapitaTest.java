package itba.pod;

import com.hazelcast.core.IList;
import itba.pod.api.model.Neighbourhood;
import itba.pod.client.queries.TreesPerCapita;
import itba.pod.client.utils.HazelCast;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.Assert.assertEquals;


public class TreesPerCapitaTest {
    private final TreesPerCapita query1 = new TreesPerCapita();
    private IList<String> trees;
    private final Map<String, Neighbourhood> neighbourhoods = new LinkedHashMap<>();

    @Before
    public void setupHazelcast() {
        List<String> addresses = List.of("127.0.0.1");
        HazelCast hz = new HazelCast(addresses);
        trees = hz.getList("g9treesPerPop");
        query1.setHazelcast(hz);
    }

    @Test
    public void testTreesPerCapita() throws ExecutionException, InterruptedException {
        Map<String, Double> expected = new LinkedHashMap<>();
        expected.put("9",0.75);
        expected.put("10",0.50);
        expected.put("11",0.33);
        expected.put("12",0.33);

        neighbourhoods.put("9",new Neighbourhood("9",4L));
        neighbourhoods.put("11",new Neighbourhood("11",3L));
        neighbourhoods.put("12",new Neighbourhood("12",3L));
        neighbourhoods.put("10",new Neighbourhood("10",10L));

        trees.add("12");
        trees.add("10");trees.add("10");trees.add("10");trees.add("10");trees.add("10");
        trees.add("9");trees.add("9");trees.add("9");
        trees.add("11");

        Map<String, Long> query = query1.mapReduce(trees);
        Stream<Map.Entry<String, Double>> sortedResult = query1.filterResult(query, neighbourhoods);

        LinkedHashMap<String,Double> map =
                sortedResult.collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1,v2)->v1,
                        LinkedHashMap::new));

        assertEquals(expected, map);
    }
}
