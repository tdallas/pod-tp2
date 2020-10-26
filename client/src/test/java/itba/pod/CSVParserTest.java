package itba.pod;

import itba.pod.api.model.CSVEntry;
import itba.pod.api.model.Neighbourhood;
import itba.pod.api.model.Tree;
import itba.pod.client.utils.CSVParser;

import itba.pod.client.utils.GetPropertyValues;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVParserTest {
    private static final CSVParser parser = new CSVParser();
    private final GetPropertyValues properties = new GetPropertyValues();
    private static final String resourcesPath = "src/test/resources/";

    @Test
    public void testReadTreesCSV() {
        String city = "BUE";

        Map<String, String> map = new HashMap<>();
        map.put(Tree.NEIGHBOURHOOD, properties.getPropValue(city + '.' + Tree.NEIGHBOURHOOD));
        map.put(Tree.STREET, properties.getPropValue(city + '.' + Tree.STREET));
        map.put(Tree.SCIENTIFIC_NAME, properties.getPropValue(city + '.' + Tree.SCIENTIFIC_NAME));
        map.put(Tree.DIAMETER, properties.getPropValue(city + '.' + Tree.DIAMETER));

        final int rowsWithAllFields = 1;    // First row
        final int rowsMissingPointlessFields = 1;   // Last row
        final int expectedReadRows = rowsWithAllFields + rowsMissingPointlessFields;

        try {
            List<CSVEntry> csvEntryList = parser.readTreesCSV(city, resourcesPath, map);

            assertEquals(expectedReadRows, csvEntryList.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReadNeighbourhoodsCSV() {
        String city = "BUE";

        Map<String, String> map = new HashMap<>();
        map.put(Neighbourhood.NAME, properties.getPropValue(Neighbourhood.NAME));
        map.put(Neighbourhood.POPULATION, properties.getPropValue(Neighbourhood.POPULATION));

        final int expectedReadRows = 1;

        try {
            List<CSVEntry> csvEntryList = parser.readNeighbourhoodsCSV(city, resourcesPath, map);

            assertEquals(expectedReadRows, csvEntryList.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
