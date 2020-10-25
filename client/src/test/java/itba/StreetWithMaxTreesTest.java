package itba;

import com.hazelcast.core.IList;
import itba.pod.api.model.Tree;
import itba.pod.api.utils.PairNeighbourhoodStreet;
import itba.pod.client.queries.StreetWithMaxTrees;
import itba.pod.client.utils.HazelCast;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertEquals;

public class StreetWithMaxTreesTest {
    HazelCast hz;
    IList<PairNeighbourhoodStreet> streetAndNeighbourhood;

    @Before
    public void createTrees() {
        List<String> addresses = new LinkedList<>();

        addresses.add("127.0.0.1");

        hz = new HazelCast(addresses);
        streetAndNeighbourhood = hz.getList("g9streetMaxTrees");
    }

    @Test
    public void StreetMaxTrees() throws ExecutionException, InterruptedException {
        Map<PairNeighbourhoodStreet,Long> expected=new LinkedHashMap<>();
        expected.put(new PairNeighbourhoodStreet("ABC","11"),3L);
        expected.put(new PairNeighbourhoodStreet("ABC","12"),3L);


        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("ABC","12"));
        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("ABC","12"));
        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("DEF","11"));
        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("ABC","11"));
        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("DEF","11"));
        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("ABC","12"));
        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("ABC","11"));
        streetAndNeighbourhood.add(new PairNeighbourhoodStreet("ABC","11"));

        List<Map.Entry<PairNeighbourhoodStreet, Long>> result =StreetWithMaxTrees.query(hz, streetAndNeighbourhood, 1);
        Map<PairNeighbourhoodStreet,Long> filtered_result=StreetWithMaxTrees.filtered_result(result);

        assertEquals(expected, filtered_result);











    }
}
