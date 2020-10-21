package itba.pod.client.utils;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.JobTracker;

import java.util.List;

public class HazelCast {

    private final HazelcastInstance instance;

    public HazelCast(List<String> addresses) {
        final ClientConfig config = new ClientConfig();
        // TODO setear usuario y contraseña
        config.getNetworkConfig().setAddresses(addresses);
        this.instance = HazelcastClient.newHazelcastClient(config);
    }

    public <T> IList<T> getList(String name) {
        IList<T> list = instance.getList(name);
        list.clear();
//        lists.add(list);
        return list;
    }

    public <K, V> IMap<K, V> getMap(String name) {
        IMap<K,V> map = instance.getMap(name);
        map.clear();
//        maps.add(map);
        return map;
    }

    public JobTracker getJobTracker(String name) {
        return instance.getJobTracker(name);
    }
}
