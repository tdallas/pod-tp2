package itba.pod;

import com.hazelcast.core.IList;
import itba.pod.api.utils.PairNeighbourhoodStreet;
import itba.pod.client.queries.StreetWithMaxTrees;
import itba.pod.client.utils.HazelCast;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.assertEquals;

public class StreetWithMaxTreesTest {
    private final StreetWithMaxTrees query2 = new StreetWithMaxTrees();
    private IList<PairNeighbourhoodStreet> streetAndNeighbourhood;

    @Before
    public void setupHazelcast() {
        List<String> addresses = List.of("127.0.0.1");
        HazelCast hz = new HazelCast(addresses);
        streetAndNeighbourhood = hz.getList("g9streetMaxTrees");
        query2.setHazelcast(hz);
    }

    @Test
    public void StreetMaxTrees() throws ExecutionException, InterruptedException {
        Map<PairNeighbourhoodStreet,Long> expected = new LinkedHashMap<>();
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

        List<Map.Entry<PairNeighbourhoodStreet, Long>> result = query2.mapReduce(streetAndNeighbourhood, 1);
        Map<PairNeighbourhoodStreet,Long> filteredResult = query2.filterResult(result);

        assertEquals(expected.size(),filteredResult.size());
        assertEquals(expected, filteredResult);
   }
}
