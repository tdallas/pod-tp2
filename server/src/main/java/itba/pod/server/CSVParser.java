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

    public static final String DELIMITER = ",";

    public List<CSVEntry> readCSV(final String dataName, final String city, final Map<String, String> fieldsMap)
            throws IOException {
//        final String filePath = "/afs/it.itba.edu.ar/pub/pod/" + dataName + city + ".csv";
        final String filePath = "/home/lucas/Documents/pod-tp2/server/src/test/java/itba/" + dataName + city + ".csv";
        BufferedReader br = Files.newBufferedReader(Paths.get(filePath));
        String line = br.readLine();
        Map<String, Integer> indexes = getIndexesFromFields(line.split(DELIMITER), fieldsMap);
        List<CSVEntry> csvEntryList = new LinkedList<>();

        if (dataName.equals(TREES)) {
            while ((line = br.readLine()) != null) {
                csvEntryList.add(buildTreeFromRow(line.split(DELIMITER), indexes));
            }
        } else if (dataName.equals(NEIGHBOURHOODS)) {
            while ((line = br.readLine()) != null) {
                csvEntryList.add(buildNeighbourhoodFromRow(line.split(DELIMITER), indexes));
            }
        } else {
            throw new IllegalArgumentException("The solicited data cannot be processed or queried due to a lack of " +
                    "implementation");
        }

        return csvEntryList;
    }

    private Tree buildTreeFromRow(final String[] row, Map<String, Integer> indexes) {
        return new Tree(
                row[indexes.get(NEIGHBOURHOOD)],
                row[indexes.get(STREET)],
                row[indexes.get(SCIENTIFIC_NAME)],
                Double.parseDouble(row[indexes.get(DIAMETER)]));
    }

    private Neighbourhood buildNeighbourhoodFromRow(final String[] row, Map<String, Integer> indexes) {
        return new Neighbourhood(
                row[indexes.get(NAME)],
                Integer.parseInt(row[indexes.get(POPULATION)]));
    }

    private Map<String, Integer> getIndexesFromFields(final String[] allFieldsArray,
                                                      final Map<String, String> stdFieldNamesMap) {
        List<String> allFields = Arrays.asList(allFieldsArray);
        Map<String, Integer> fieldIndexMap = new HashMap<>();

        for (Map.Entry<String, String> e : stdFieldNamesMap.entrySet())
            fieldIndexMap.put(e.getKey(), allFields.indexOf(e.getValue()));

        return fieldIndexMap;
    }
}
