package itba;

import com.hazelcast.core.IList;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import itba.pod.api.model.Tree;
import itba.pod.client.queries.TopSpeciesWithMaxDiam;
import itba.pod.client.utils.HazelCast;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TopSpeciesWithMaxDiamTest {

    @Test
    public void testQuery() throws ExecutionException, InterruptedException {
        List<String> addresses = new LinkedList<>();
        addresses.add("127.0.0.1");
        HazelCast hz = new HazelCast(addresses);
        IList<Tree> trees = hz.getList("g9topNSpecies");
        var t1 = new Tree();
        var t2 = new Tree();
        var t3 = new Tree();
        var t4 = new Tree();

        t1.setScientificName("Jacarandá");
        t1.setDiameter(110.0);
        t2.setScientificName("Abedul");
        t2.setDiameter(40.0);
        t3.setScientificName("Lapacho");
        t3.setDiameter(85.0);
        t4.setScientificName("Jacarandá");
        t4.setDiameter(85.0);
        trees.add(new Tree(null, null, "Laurel", 85d));
        trees.add(t1);
        trees.add(t2);
        trees.add(t3);
        trees.add(t4);

//        trees.add(new Tree("", "","Jacarandá", 110.0));
//        trees.add(new Tree("", "", "Abedul", 40.0));
//        trees.add(new Tree("", "", "Lapacho", 85.0));
//        trees.add(new Tree("", "", "Jacarandá", 85.0));

        System.out.println(trees);
        try {
            List<Map.Entry<String, Double>> map = TopSpeciesWithMaxDiam.query(hz, trees, 2);
            System.out.println(map);
        } catch (HazelcastSerializationException e) {
            e.printStackTrace();
        }
    }
}
