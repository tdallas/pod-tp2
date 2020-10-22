package itba;

import itba.pod.api.model.CSVEntry;
import itba.pod.api.model.Neighbourhood;
import itba.pod.api.model.Tree;
import itba.pod.client.utils.CSVParser;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVParserTest {
    public static final CSVParser parser = new CSVParser();
    public static final String resourcesPath = "/home/lucas/Documents/pod-tp2/server/src/test/resources/";

    @Test
    // TODO: Add assert
    public void fakeTestReadTreesCSV() {
        Map<String, String> map = new HashMap<>();

        map.put(Tree.NEIGHBOURHOOD, "comuna");
        map.put(Tree.STREET, "calle_nombre");
        map.put(Tree.SCIENTIFIC_NAME, "nombre_cientifico");
        map.put(Tree.DIAMETER, "diametro_altura_pecho");

        try {
            List<CSVEntry> csvEntryList = parser.readTreesCSV("BUE", resourcesPath, map);

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
        Map<String, String> map = new HashMap<>();

        map.put(Neighbourhood.NAME, "nombre");
        map.put(Neighbourhood.POPULATION, "habitantes");

        try {
            List<CSVEntry> csvEntryList = parser.readNeighbourhoodsCSV("BUE", resourcesPath, map);

            for (CSVEntry csvEntry : csvEntryList) {
                System.out.println(csvEntry.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
