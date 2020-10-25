package itba.pod.client.exceptions;

public class InvalidArgumentException extends Exception {
    public InvalidArgumentException(String errorMessage) {
        super(errorMessage);
    }
}
