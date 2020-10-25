package itba.pod.client.utils;

import itba.pod.client.exceptions.InvalidArgumentException;

public class ArgumentValidator {
    public static void validate(String addresses, String city, String inPath, String outPath) throws InvalidArgumentException {
        validateNotNull("-Daddresses", addresses);
        validateCity(city);
        validateNotNull("-DinPath", inPath);
        validateNotNull("-DoutPath", outPath);
    }

    public static void validate(String addresses, String city, String inPath, String outPath, String num) throws InvalidArgumentException {
        validate(addresses, city, inPath, outPath);
        validatePositiveInteger(num);
    }

    public static void validate(String addresses, String city, String inPath, String outPath, String minTreesString, String species) throws InvalidArgumentException {
        validate(addresses, city, inPath, outPath);
        validatePositiveInteger(minTreesString);
        validateNotNull("-Dname", species);
    }

    private static void validateNotNull(String argumentName, String argument) throws InvalidArgumentException {
        if (argument == null)
            throw new InvalidArgumentException(argumentName + " argument is required");
    }

    private static void validateCity(String city) throws InvalidArgumentException {
        validateNotNull("-Dcity", city);

        if (!city.equals("BUE") && !city.equals("VAN"))
            throw new InvalidArgumentException("-Dcity should be BUE or VAN");
    }

    private static void validatePositiveInteger(String num) throws InvalidArgumentException {
        validateNotNull("number", num);

        try {
            int integer = Integer.parseInt(num);

            if (integer < 0) throw new InvalidArgumentException("number should be a positive integer");
        } catch (NumberFormatException e) {
            throw new InvalidArgumentException("number should be a positive integer");
        }
    }
}
