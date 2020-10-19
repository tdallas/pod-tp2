package itba.pod.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {
        logger.info("tpe2-g9-parent Client Starting ...");
        ClientConfig config = new ClientConfig();
        config.setInstanceName("dev");
        HazelcastInstance hazelcastInstanceClient = HazelcastClient.newHazelcastClient(config);
        Map<Long, String> map = hazelcastInstanceClient.getMap("data");
        for (Map.Entry<Long, String> entry : map.entrySet()) {
	        System.out.println(entry.getKey() + entry.getValue());
        }
    }
}
