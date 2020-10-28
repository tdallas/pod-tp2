package itba.pod.server;

import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("tpe2-g9-parent Server Starting ...");
        Hazelcast.newHazelcastInstance();
    }
}
