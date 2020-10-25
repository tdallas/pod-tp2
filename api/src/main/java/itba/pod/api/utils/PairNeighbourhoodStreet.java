package itba.pod.api.utils;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class PairNeighbourhoodStreet implements Comparable<PairNeighbourhoodStreet>, DataSerializable {
    private String street;
    private String neighbourhood;

    public PairNeighbourhoodStreet(String street, String neighbourhood) {
        this.street = street;
        this.neighbourhood = neighbourhood;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public void setNeighbourhood(String neighbourhood) {
        this.neighbourhood = neighbourhood;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairNeighbourhoodStreet that = (PairNeighbourhoodStreet) o;
        return Objects.equals(street, that.street) &&
                Objects.equals(neighbourhood, that.neighbourhood);
    }

    @Override
    public int hashCode() {
        return Objects.hash(street, neighbourhood);
    }

    @Override
    public int compareTo(PairNeighbourhoodStreet pns) {
        return this.neighbourhood.compareTo(pns.neighbourhood);
    }


    public PairNeighbourhoodStreet clone() {
        return new PairNeighbourhoodStreet(new String(street), new String(neighbourhood));
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(this.neighbourhood);
        objectDataOutput.writeUTF(this.street);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.neighbourhood=objectDataInput.readUTF();
        this.street=objectDataInput.readUTF();
    }
}
