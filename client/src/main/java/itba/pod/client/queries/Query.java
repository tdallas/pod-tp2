package itba.pod.client.queries;

import itba.pod.api.model.Neighbourhood;
import itba.pod.api.model.Tree;
import itba.pod.client.exceptions.InvalidArgumentException;
import itba.pod.client.utils.ArgumentValidator;
import itba.pod.client.utils.CSVParser;
import itba.pod.client.utils.HazelCast;
import itba.pod.client.utils.OutputFileWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Query {
    private String addresses;
    public String city;
    public String inPath;
    private String outPath;
    public OutputFileWriter fileWriter;
    public HazelCast hz;

    public void setup(final int queryNumber) throws InvalidArgumentException {
        readArguments();

        List<String> addressesList = Arrays.asList(addresses.split(";"));
        fileWriter = new OutputFileWriter(outPath, queryNumber);
        hz = new HazelCast(addressesList);
    }

    public List<Tree> readTrees() throws IOException {
        CSVParser parser = new CSVParser();
        fileWriter.timestampBeginFileRead();
        List<Tree> trees = parser.readTrees(inPath, city);
        fileWriter.timestampEndFileRead();

        return trees;
    }

    public Map<String, Neighbourhood> readNeighbourhoods() throws IOException {
        CSVParser parser = new CSVParser();
        fileWriter.timestampBeginFileRead();
        Map<String, Neighbourhood> neighbourhoods = parser.readNeighbourhoods(inPath, city);
        fileWriter.timestampEndFileRead();

        return neighbourhoods;
    }

    private void readArguments() throws InvalidArgumentException {
        addresses = System.getProperty("addresses");
        city = System.getProperty("city");
        inPath = System.getProperty("inPath");
        outPath = System.getProperty("outPath");

        ArgumentValidator.validate(addresses, city, inPath, outPath);
    }

    public void setHazelcast(HazelCast hz) {
        this.hz = hz;
    }
}
