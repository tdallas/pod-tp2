package itba.pod;

import itba.pod.api.model.CSVEntry;
import itba.pod.client.utils.CSVHeaderParser;
import itba.pod.client.utils.CSVParser;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

public class CSVParserTest {
    private static final CSVParser parser = new CSVParser();
    private static final CSVHeaderParser headerParser = new CSVHeaderParser();
    private static final String resourcesPath = "src/test/resources/";

    @Test
    public void testReadTreesCSV() {
        String city = "BUE";
        Map<String, String> map = new HashMap<>();

        try {
            map = headerParser.readTreeHeaders(city);
        } catch (IOException e) {
            e.printStackTrace();
        }

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

        try {
            map = headerParser.readNeighbourhoodHeaders(city);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final int expectedReadRows = 1;

        try {
            List<CSVEntry> csvEntryList = parser.readNeighbourhoodsCSV(city, resourcesPath, map);

            assertEquals(expectedReadRows, csvEntryList.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
