package itba.pod.client.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import itba.pod.api.model.Neighbourhood;
import itba.pod.api.model.Tree;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CSVHeaderParser {
    private final Path headersConfigFilePath;

    public CSVHeaderParser() {
        String basePathString = new File("").getAbsolutePath();
        headersConfigFilePath = Paths.get(basePathString + "/config/cities.json");
//        Use this path when running with IntelliJ
//        headersConfigFilePath = Paths.get(basePathString + "/client/src/main/resources/cities.json");
    }

    public Map<String, String> readTreeHeaders(final String city) throws IOException {
        return readStandardHeadersFromJSON(city, Tree.getStandardHeaders(), "TREES");
    }

    public Map<String, String> readNeighbourhoodHeaders(final String city) throws IOException {
        return readStandardHeadersFromJSON(city, Neighbourhood.getStandardHeaders(), "NEIGHBOURHOODS");
    }

    private Map<String, String> readStandardHeadersFromJSON(final String city, final List<String> standardHeaders,
                                                            final String type) throws IOException {
        Map<String, String> standardHeadersToCustomHeadersMap = new HashMap<>();
        JsonNode jsonTree = loadJsonTree();
        JsonNode cityNode = jsonTree.get(city).get(type);

        if (cityNode == null)
            throw new IOException("No header mappings found for '" + type + "' in '" + city + "', " +
                    "please add the missing headers");

        for (String header : standardHeaders)
            standardHeadersToCustomHeadersMap.put(header, cityNode.get(header).asText());

        return standardHeadersToCustomHeadersMap;
    }

    private JsonNode loadJsonTree() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readTree(new File(headersConfigFilePath.toString()));
    }

    public Set<String> getAcceptableCities() throws IOException {
        Set<String> acceptableCities = new HashSet<>();
        JsonNode jsonTree = loadJsonTree();
        var jsonIterator = jsonTree.fields();

        while (jsonIterator.hasNext())
            acceptableCities.add(jsonIterator.next().getKey());

        return acceptableCities;
    }
}
