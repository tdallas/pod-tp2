package itba.pod.client.utils;

import itba.pod.api.utils.PairNeighbourhoodStreet;
import itba.pod.api.utils.SortedPair;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

public class OutputFiles {
    String outputFilePath;

    public OutputFiles(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    // TODO ver si integers y longs se imprimen bien

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


    //query 1
    public void treesPerPopulationWriter(Stream<Map.Entry<String, Double>> results) {
        try (FileWriter fw = new FileWriter(outputFilePath + "/query1.csv")) {
            StringBuilder sb = new StringBuilder();
            sb.append("BARRIO;ARBOLES_POR_HABITANTE\n");
            results.forEach((k) -> sb.append(k.getKey()).append(";").append(String.format("%.2f", k.getValue())).append("\n"));
            fw.write(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //query 2
    public void StreetWithMaxTreesWriter(Map<PairNeighbourhoodStreet, Long> results) {
        try (FileWriter fw = new FileWriter(outputFilePath + "/query2.csv")) {
            StringBuilder sb = new StringBuilder();
            sb.append("BARRIO;CALLE_CON_MAS_ARBOLES;ARBOLES\n");
            results.forEach((k, v) -> sb.append(k.getNeighbourhood()).append(";").append(k.getStreet()).append(";").append(v).append("\n"));
            fw.write(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //query 3
    public void TopSpeciesWithMaxDiamWriter(List<Map.Entry<String, Double>> results) {
        try (FileWriter fw = new FileWriter(outputFilePath + "/query3.csv")) {
            StringBuilder sb = new StringBuilder();
            sb.append("NOMBRE_CIENTIFICO;PROMEDIO_DIAMETRO\n");
            results.forEach((k) -> sb.append(k.getKey()).append(";").append(String.format("%.2f", k.getValue())).append("\n"));
            fw.write(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //query 5
    public void NeighbourhoodsPairsWithThousandTreesWriter(List<Map.Entry<Long, SortedPair<String>>> results) {
        try (FileWriter fw = new FileWriter(outputFilePath + "/query5.csv")) {
            StringBuilder sb = new StringBuilder();
            sb.append("Grupo;Barrio A;Barrio B\n");
            results.forEach((k) -> sb.append(k.getKey()).append(";").append(k.getValue().getA()).append(";").append(k.getValue().getB()).append("\n"));
            fw.write(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
