package itba;

import itba.pod.server.CSVEntry;
import itba.pod.server.CSVParser;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVParserTest {
    public static final CSVParser parser = new CSVParser();

    @Test
    public void testReadCSV() {
        Map<String, String> map = new HashMap<>();

        map.put(CSVParser.NEIGHBOURHOOD, "comuna");
        map.put(CSVParser.STREET, "calle_nombre");
        map.put(CSVParser.SCIENTIFIC_NAME, "nombre_cientifico");
        map.put(CSVParser.DIAMETER, "diametro_altura_pecho");

        try {
            List<CSVEntry> csvEntryList = parser.readCSV("arboles", "BUE", map);

            for (CSVEntry csvEntry : csvEntryList)
                System.out.println(csvEntry.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
