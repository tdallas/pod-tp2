package itba.pod;

import itba.pod.api.model.CSVEntry;
import itba.pod.api.model.Neighbourhood;
import itba.pod.api.model.Tree;
import itba.pod.client.utils.CSVParser;

import itba.pod.client.utils.GetPropertyValues;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVParserTest {
    private static final CSVParser parser = new CSVParser();
    private GetPropertyValues properties = new GetPropertyValues();
    private static final String resourcesPath = "src/test/resources/";

    @Test
    // TODO: Add assert
    public void fakeTestReadTreesCSV() {
        String city = "BUE";

        Map<String, String> map = new HashMap<>();
        map.put(Tree.NEIGHBOURHOOD, properties.getPropValue(city + '.' + Tree.NEIGHBOURHOOD));
        map.put(Tree.STREET, properties.getPropValue(city + '.' + Tree.STREET));
        map.put(Tree.SCIENTIFIC_NAME, properties.getPropValue(city + '.' + Tree.SCIENTIFIC_NAME));
        map.put(Tree.DIAMETER, properties.getPropValue(city + '.' + Tree.DIAMETER));

        try {
            List<CSVEntry> csvEntryList = parser.readTreesCSV(city, resourcesPath, map);

            for (CSVEntry csvEntry : csvEntryList) {
                System.out.println(csvEntry.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    // TODO: Add assert// TODO: Add assert
    public void fakeTestReadNeighbourhoodsCSV() {
        String city = "BUE";

        Map<String, String> map = new HashMap<>();
        map.put(Neighbourhood.NAME, properties.getPropValue(Neighbourhood.NAME));
        map.put(Neighbourhood.POPULATION, properties.getPropValue(Neighbourhood.POPULATION));

        try {
            List<CSVEntry> csvEntryList = parser.readNeighbourhoodsCSV(city, resourcesPath, map);

            for (CSVEntry csvEntry : csvEntryList) {
                System.out.println(csvEntry.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
