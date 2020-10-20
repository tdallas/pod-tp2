package itba.pod.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class CSVParser {
    // Tree fields (standard name)
    public static final String NEIGHBOURHOOD = "NEIGHBOURHOOD";
    public static final String STREET = "STREET";
    public static final String SCIENTIFIC_NAME = "SCIENTIFIC_NAME";
    public static final String DIAMETER = "DIAMETER";

    // Neighbourhood fields (standard name)
    public static final String NAME = "NAME";
    public static final String POPULATION = "POPULATION";

    // Possible data types to process and query
    public static final String TREES = "arboles";
    public static final String NEIGHBOURHOODS = "barrios";

    public static final String DELIMITER = ";";

    public List<CSVEntry> readCSV(final String dataName, final String city, final Map<String, String> fieldsMap)
            throws IOException {
//        final String filePath = "/afs/it.itba.edu.ar/pub/pod/" + dataName + city + ".csv";
        // TODO: Change this path
        final String filePath = "/home/lucas/Documents/pod-tp2/server/src/test/java/itba/" + dataName + city + ".csv";
        BufferedReader br = Files.newBufferedReader(Paths.get(filePath));
        final String[] header = br.readLine().split(DELIMITER);
        Map<String, Integer> indexes = getIndexesFromFields(header, fieldsMap);
        List<CSVEntry> csvEntryList;

        if (dataName.equals(TREES)) {
            csvEntryList = getTreesFromCSV(br, indexes, header.length);
        } else if (dataName.equals(NEIGHBOURHOODS)) {
            csvEntryList = getNeighbourhoodsFromCSV(br, indexes, header.length);
        } else {
            throw new IllegalArgumentException("The solicited data cannot be processed or queried due to a lack of " +
                    "implementation");
        }

        return csvEntryList;
    }

    private List<CSVEntry> getTreesFromCSV(BufferedReader br, final Map<String, Integer> indexes,
                                           final Integer columns) throws IOException {
        List<CSVEntry> csvEntryList = new LinkedList<>();
        String line;

        while ((line = br.readLine()) != null) {
            if (line.length() < columns)
                continue;

            Optional<Tree> tree = buildTreeFromRow(line.split(DELIMITER), indexes);

            tree.ifPresent(csvEntryList::add);
        }

        return csvEntryList;
    }

    private List<CSVEntry> getNeighbourhoodsFromCSV(BufferedReader br, final Map<String, Integer> indexes,
                                                    final Integer columns) throws IOException {
        List<CSVEntry> csvEntryList = new LinkedList<>();
        String line;

        while ((line = br.readLine()) != null) {
            if (line.length() < columns)
                continue;

            Optional<Neighbourhood> neighbourhood = buildNeighbourhoodFromRow(line.split(DELIMITER), indexes);

            neighbourhood.ifPresent(csvEntryList::add);
        }

        return csvEntryList;
    }

    private Map<String, Integer> getIndexesFromFields(final String[] allFieldsArray,
                                                      final Map<String, String> stdFieldNamesMap) {
        List<String> allFields = Arrays.asList(allFieldsArray);
        Map<String, Integer> fieldIndexMap = new HashMap<>();

        for (Map.Entry<String, String> e : stdFieldNamesMap.entrySet())
            fieldIndexMap.put(e.getKey(), allFields.indexOf(e.getValue()));

        return fieldIndexMap;
    }

    private Optional<Tree> buildTreeFromRow(final String[] row, Map<String, Integer> indexes) {
        String neighbourhood = row[indexes.get(NEIGHBOURHOOD)];
        String street = row[indexes.get(STREET)];
        String scientificName = row[indexes.get(SCIENTIFIC_NAME)];
        String diameterString = row[indexes.get(DIAMETER)];

        if (neighbourhood.isEmpty() || street.isEmpty() || scientificName.isEmpty() || diameterString.isEmpty())
            return Optional.empty();

        return Optional.of(new Tree(neighbourhood, street, scientificName, Double.parseDouble(diameterString)));
    }

    private Optional<Neighbourhood> buildNeighbourhoodFromRow(final String[] row, Map<String, Integer> indexes) {
        String name = row[indexes.get(NAME)];
        String populationString = row[indexes.get(POPULATION)];

        if (name.isEmpty() || populationString.isEmpty())
            return Optional.empty();

        return Optional.of(new Neighbourhood(name, Integer.parseInt(populationString)));
    }
}
