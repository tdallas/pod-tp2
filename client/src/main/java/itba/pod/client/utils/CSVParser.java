package itba.pod.client.utils;

import itba.pod.api.model.CSVEntry;
import itba.pod.api.model.Neighbourhood;
import itba.pod.api.model.Tree;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class CSVParser {

    // Possible data types to process and query
    private static final String TREES = "arboles";
    private static final String NEIGHBOURHOODS = "barrios";

    private static final String DELIMITER = ";";

    private final GetPropertyValues properties;

    public CSVParser() {
         properties = new GetPropertyValues();
    }

    public Map<String, Neighbourhood> readNeighbourhoods(String inPath, String city) throws IOException {
        Map<String, String> fieldsMap = new HashMap<>();
        fieldsMap.put(Neighbourhood.NAME, properties.getPropValue(Neighbourhood.NAME));
        fieldsMap.put(Neighbourhood.POPULATION, properties.getPropValue(Neighbourhood.POPULATION));

        Map<String, Neighbourhood> result = new HashMap<>();
        for (CSVEntry entry : readNeighbourhoodsCSV(city, inPath, fieldsMap)) {
            Neighbourhood neighbourhood = (Neighbourhood) entry;
            result.put(neighbourhood.getName(), neighbourhood);
        }
        return result;
    }

    public List<Tree> readTrees(String inPath, String city) throws IOException {
        Map<String, String> fieldsMap = new HashMap<>();
        fieldsMap.put(Tree.NEIGHBOURHOOD, properties.getPropValue(city + '.' + Tree.NEIGHBOURHOOD));
        fieldsMap.put(Tree.STREET, properties.getPropValue(city + '.' + Tree.STREET));
        fieldsMap.put(Tree.SCIENTIFIC_NAME, properties.getPropValue(city + '.' + Tree.SCIENTIFIC_NAME));
        fieldsMap.put(Tree.DIAMETER, properties.getPropValue(city + '.' + Tree.DIAMETER));

        List<Tree> result = new LinkedList<>();
        for (CSVEntry entry : readTreesCSV(city, inPath, fieldsMap)) {
            result.add((Tree)entry);
        }
        return result;
    }

    /**
     * Reads data from a CSV file and returns it as a list of entries. It will ignore rows that do not have all the
     * expected fields to process the data or that have empty fields for said data.
     * @param city is the abbreviated name of a city, e.g., "BUE" for Buenos Aires or "NYC" for New York.
     * @param path is the path of the folder containing the input CSV file
     * @param fieldsMap is the map <standardName, customName> indicating what custom field names correspond to which
     *                  standard names for the data values.
     * @return a list of entries. Every entry in the list will be of the same type.
     * @throws IOException when the buffered reader fails to read a line.
     */
    public List<CSVEntry> readTreesCSV(final String city, final String path, final Map<String, String> fieldsMap)
            throws IOException {
        final String filePath = path + TREES + city + ".csv";

        return readCSV(TREES, filePath, fieldsMap);
    }

    /**
     * Reads data from a CSV file and returns it as a list of entries. It will ignore rows that do not have all the
     * expected fields to process the data or that have empty fields for said data.
     * @param city is the abbreviated name of a city, e.g., "BUE" for Buenos Aires or "NYC" for New York.
     * @param path is the path of the folder containing the input CSV file
     * @param fieldsMap is the map <standardName, customName> indicating what custom field names correspond to which
     *                  standard names for the data values.
     * @return a list of entries. Every entry in the list will be of the same type.
     * @throws IOException when the buffered reader fails to read a line.
     */
    public List<CSVEntry> readNeighbourhoodsCSV(final String city, final String path, final Map<String, String> fieldsMap)
            throws IOException {
        final String filePath = path + NEIGHBOURHOODS + city + ".csv";

        return readCSV(NEIGHBOURHOODS, filePath, fieldsMap);
    }

    /**
     * Reads data from a CSV file and returns it as a list of entries. It will ignore rows that do not have all the
     * expected fields to process the data or that have empty fields for said data.
     * @param dataName is the plural name of the data in Spanish, e.g., "arboles" for data about trees. Check the static
     *                 fields of the parser in order to know what type of data you can query.
     * @param filePath is the input CSV's full file path.
     * @param fieldsMap is the map <standardName, customName> indicating what custom field names correspond to which
     *                  standard names for the data values.
     * @return a list of entries. Every entry in the list will be of the same type.
     * @throws IOException when the buffered reader fails to read a line
     * @throws IllegalArgumentException when the input dataName doesn't match any of the data names expected to be
     *                                  queried.
     */
    private List<CSVEntry> readCSV(final String dataName, final String filePath, final Map<String, String> fieldsMap)
            throws IOException, IllegalArgumentException {
        BufferedReader br = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.ISO_8859_1);

        final String[] header = br.readLine().split(DELIMITER);
        Map<String, Integer> indexes = getIndexesFromFields(header, fieldsMap);

        return getDataFromCSV(br, indexes, dataName);
    }

    private List<CSVEntry> getDataFromCSV(BufferedReader br, final Map<String, Integer> indexes, final String dataName)
            throws IOException, IllegalArgumentException {
        List<CSVEntry> csvEntryList = new LinkedList<>();
        String line;

        while ((line = br.readLine()) != null) {
            final String[] row = line.split(DELIMITER);

            // TODO: Refactor this so that the if-else comparison inside the function isn't checked in every iteration
            Optional<CSVEntry> entry = buildEntry(row, indexes, dataName);

            entry.ifPresent(csvEntryList::add);
        }

        return csvEntryList;
    }

    private Optional<CSVEntry> buildEntry(final String[] row, Map<String, Integer> indexes, final String dataName)
            throws IllegalArgumentException {
        if (dataName.equals(TREES)) {
            return buildTreeFromRow(row, indexes);
        } else if (dataName.equals(NEIGHBOURHOODS)) {
            return buildNeighbourhoodFromRow(row, indexes);
        } else {
            throw new IllegalArgumentException("The solicited data cannot be processed or queried due to a lack of " +
                    "implementation");
        }
    }

    private Map<String, Integer> getIndexesFromFields(final String[] allFieldsArray,
                                                      final Map<String, String> stdFieldNamesMap) {
        List<String> allFields = Arrays.asList(allFieldsArray);
        Map<String, Integer> fieldIndexMap = new HashMap<>();

        for (Map.Entry<String, String> e : stdFieldNamesMap.entrySet())
            fieldIndexMap.put(e.getKey(), allFields.indexOf(e.getValue()));

        return fieldIndexMap;
    }

    private Optional<CSVEntry> buildTreeFromRow(final String[] row, Map<String, Integer> indexes) {
        String neighbourhood = row[indexes.get(Tree.NEIGHBOURHOOD)];
        String street = row[indexes.get(Tree.STREET)];
        String scientificName = row[indexes.get(Tree.SCIENTIFIC_NAME)];
        String diameterString = row[indexes.get(Tree.DIAMETER)];

        if (neighbourhood.isEmpty() || street.isEmpty() || scientificName.isEmpty() || diameterString.isEmpty() || Double.parseDouble(diameterString) <= 0)
            return Optional.empty();

        return Optional.of(new Tree(neighbourhood, street, scientificName, Double.parseDouble(diameterString)));
    }

    private Optional<CSVEntry> buildNeighbourhoodFromRow(final String[] row, Map<String, Integer> indexes) {
        String name = row[indexes.get(Neighbourhood.NAME)];
        String populationString = row[indexes.get(Neighbourhood.POPULATION)];

        if (name.isEmpty() || populationString.isEmpty() || Long.parseLong(populationString) <= 0)
            return Optional.empty();

        return Optional.of(new Neighbourhood(name, Long.parseLong(populationString)));
    }
}
