package itba.pod.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("tpe2-g9-parent Server Starting ...");
        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance();
        Map<Long, String> map = hzInstance.getMap("data");
        for (int i = 0; i < 10; i++) {
            map.put((long) i, "message" + i);
        }

    }
}
