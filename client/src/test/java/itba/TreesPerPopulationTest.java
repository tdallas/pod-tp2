package itba;

import com.hazelcast.core.IList;
import itba.pod.api.model.Neighbourhood;
import itba.pod.client.queries.TreesPerPopulation;
import itba.pod.client.utils.HazelCast;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class TreesPerPopulationTest {
    private HazelCast hz;
    private IList<String> trees;
    private Map<String, Neighbourhood> neighbourhoods=new LinkedHashMap<>();

    @Before
    public void createTrees() {
        List<String> addresses = new LinkedList<>();

        addresses.add("127.0.0.1");

        hz = new HazelCast(addresses);
        trees = hz.getList("g9treesPerPop");
    }

    @Test
    public void testTreesPerPopulation() throws ExecutionException, InterruptedException {
        Map<String, Double> expected = new LinkedHashMap<>();
        expected.put("9",0.75);
        expected.put("10",0.50);
        expected.put("11",1/(double)3);
        expected.put("12",1/(double)3);

        neighbourhoods.put("9",new Neighbourhood("9",4));
        neighbourhoods.put("11",new Neighbourhood("11",3));
        neighbourhoods.put("12",new Neighbourhood("12",3));
        neighbourhoods.put("10",new Neighbourhood("10",10));

        trees.add("12");
        trees.add("10");trees.add("10");trees.add("10");trees.add("10");trees.add("10");
        trees.add("9");trees.add("9");trees.add("9");
        trees.add("11");

        Map<String, Long> query=TreesPerPopulation.query(hz,trees);
        Stream<Map.Entry<String, Double>> sorted_result= TreesPerPopulation.filterResult(query,neighbourhoods);

        LinkedHashMap<String,Double> map =
                sorted_result.collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1,v2)->v1,
                        LinkedHashMap::new));

        assertEquals(expected, map);

    }
}
