package itba.pod.client.exceptions;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class QueryException extends Exception {
    private final Exception specificException;

    public QueryException(final Exception specificException) {
        this.specificException = specificException;
    }

    public void dealWithSpecificException(final Logger LOGGER) {
        if (specificException instanceof InvalidArgumentException) {
            System.out.println(specificException.getMessage());
        } else if (specificException instanceof IOException) {
            LOGGER.error("Caught an error reading the CSV or configuration file\n");
            System.out.println(specificException.getMessage());
        } else if (specificException instanceof InterruptedException ||
                specificException instanceof ExecutionException) {
            LOGGER.error("Caught an error in the map/reduce process\n");
            specificException.printStackTrace();
        }
    }
}
