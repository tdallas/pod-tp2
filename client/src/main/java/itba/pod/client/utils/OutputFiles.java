package itba.pod.client.utils;

import itba.pod.api.utils.PairNeighbourhoodStreet;
import itba.pod.api.utils.SortedPair;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class OutputFiles {
    String outputFilePath;
    private final StringBuilder sb = new StringBuilder();
    private final String DELIMITER = ";";

    public OutputFiles(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    public void timeStampFile(String work, int queryN) {
//        https://mkyong.com/java8/java-8-how-to-format-localdatetime/
        try (FileWriter fw = new FileWriter(outputFilePath + "query" + queryN + ".txt", true)) {
            StringBuilder sb = new StringBuilder();
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-mm-yyyy hh:mm:ss:xxxx");
            String formatDateTime = now.format(formatter);
            sb.append(formatDateTime).append("INFO Client - ").append(work).append("\n");
            fw.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeTreesPerCapita(Stream<Map.Entry<String, Double>> results) {
        sb.append("BARRIO;ARBOLES_POR_HABITANTE\n");
        results.forEach((e) -> sb
                .append(e.getKey()).append(DELIMITER)
                .append(String.format("%.2f", e.getValue()))
                .append("\n"));
        this.toCSV("query1");
    }

    public void writeStreetWithMaxTrees(Map<PairNeighbourhoodStreet, Long> results) {
        sb.append("BARRIO;CALLE_CON_MAS_ARBOLES;ARBOLES\n");
        results.forEach((k, v) -> sb
                .append(k.getNeighbourhood()).append(DELIMITER)
                .append(k.getStreet()).append(DELIMITER)
                .append(v)
                .append("\n"));
        this.toCSV("query2");
    }

    public void writeTopSpeciesWithMaxDiam(List<Map.Entry<String, Double>> results) {
        sb.append("NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO\n");
        results.forEach(e -> sb
                .append(e.getKey()).append(DELIMITER)
                .append(String.format("%.2f", e.getValue()))
                .append("\n"));
        this.toCSV("query3");
    }

    public void writeNeighbourhoodPairsWithMinTrees(List<Map.Entry<String, String>> results) {
        sb.append("Barrio A;Barrio B");
        results.forEach(e -> sb
                .append(e.getKey()).append(DELIMITER)
                .append(e.getValue())
                .append("\n"));
        this.toCSV("query4");
    }

    public void writeNeighbourhoodPairsWithThousandTrees(List<Map.Entry<Long, SortedPair<String>>> results) {
        sb.append("Grupo;Barrio A;Barrio B\n");
        results.forEach(e -> sb
                .append(e.getKey()).append(DELIMITER)
                .append(e.getValue().getA()).append(DELIMITER)
                .append(e.getValue().getB())
                .append("\n"));
        this.toCSV("query5");
    }

    private void toCSV(final String fileName) {
        try (FileWriter fw = new FileWriter(outputFilePath + "/" + fileName + ".csv")) {
            fw.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
