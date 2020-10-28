package itba.pod.client.utils;

import itba.pod.client.exceptions.InvalidArgumentException;

import java.util.Set;

public class ArgumentValidator {
    public static void validate(String addresses, String city, String inPath, String outPath, Set<String> validCities)
            throws InvalidArgumentException {
        validateNotNull("-Daddresses", addresses);
        validateCity(city, validCities);
        validateNotNull("-DinPath", inPath);
        validateNotNull("-DoutPath", outPath);
    }

    public static void validateQuery2(String minTreesString) throws InvalidArgumentException {
        validatePositiveInteger("-Dmin", minTreesString);
    }

    public static void validateQuery3(String nString) throws InvalidArgumentException {
        validatePositiveInteger("-Dn", nString);
    }

    public static void validateQuery4(String minTreesString, String species) throws InvalidArgumentException {
        validatePositiveInteger("-Dmin", minTreesString);
        validateNotNull("-Dname", species);
    }

    private static void validateNotNull(String argumentName, String argument) throws InvalidArgumentException {
        if (argument == null)
            throw new InvalidArgumentException(argumentName + " argument is required");
    }

    private static void validateCity(String city, final Set<String> validCities) throws InvalidArgumentException {
        validateNotNull("-Dcity", city);

        if (!validCities.contains(city))
            throw new InvalidArgumentException("-Dcity should be one of the following: " + validCities.toString());
    }

    private static void validatePositiveInteger(String argumentName, String num) throws InvalidArgumentException {
        validateNotNull(argumentName, num);

        try {
            int integer = Integer.parseInt(num);

            if (integer < 0) throw new InvalidArgumentException(argumentName + " should be a positive integer");
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }
}
