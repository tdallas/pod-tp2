package itba.pod.api.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Tree implements CSVEntry, DataSerializable {

    // Tree fields (standard name)
    public static final String NEIGHBOURHOOD = "NEIGHBOURHOOD";
    public static final String STREET = "STREET";
    public static final String SCIENTIFIC_NAME = "SCIENTIFIC_NAME";
    public static final String DIAMETER = "DIAMETER";

    private String neighbourhood;
    private String street;
    private String scientificName;
    private Double diameter;

    public Tree() {}

    public Tree(String neighbourhood, String street, String scientificName, Double diameter) {
        this.neighbourhood = neighbourhood;
        this.street = street;
        this.scientificName = scientificName;
        this.diameter = diameter;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public String getStreet() {
        return street;
    }

    public String getScientificName() {
        return scientificName;
    }

    public Double getDiameter() {
        return diameter;
    }

    @Override
    public String toString() {
        return String.join(", ", neighbourhood, street, scientificName, diameter.toString());
    }

    public static List<String> getStandardHeaders() {
        return List.of(NEIGHBOURHOOD, STREET, SCIENTIFIC_NAME, DIAMETER);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(this.neighbourhood);
        out.writeUTF(this.street);
        out.writeUTF(this.scientificName);
        out.writeDouble(this.diameter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.neighbourhood = in.readUTF();
        this.street = in.readUTF();
        this.scientificName = in.readUTF();
        this.diameter = in.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tree tree = (Tree) o;
        return neighbourhood.equals(tree.neighbourhood) &&
                street.equals(tree.street) &&
                scientificName.equals(tree.scientificName) &&
                diameter.equals(tree.diameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(neighbourhood, street, scientificName, diameter);
    }
}
