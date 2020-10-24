package itba.pod.api.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class Tree implements CSVEntry, DataSerializable {

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

    public void setNeighbourhood(String neighbourhood) {
        this.neighbourhood = neighbourhood;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public void setScientificName(String scientificName) {
        this.scientificName = scientificName;
    }

    public void setDiameter(Double diameter) {
        this.diameter = diameter;
    }
}
