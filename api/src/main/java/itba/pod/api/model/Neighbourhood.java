package itba.pod.api.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;


import java.io.IOException;
import java.util.Objects;

public class Neighbourhood implements CSVEntry, DataSerializable {


    // Neighbourhood fields (standard name)
    public static final String NAME = "NAME";
    public static final String POPULATION = "POPULATION";

    private String name;
    private Long population;

    public Neighbourhood(String name, Long population) {
        this.name = name;
        this.population = population;
    }

    public String getName() {
        return name;
    }

    public Long getPopulation() {
        return population;
    }

    @Override
    public String toString() {
        return String.join(", ", name, population.toString());
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(this.name);
        objectDataOutput.writeLong(this.population);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.name = objectDataInput.readUTF();
        this.population = objectDataInput.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Neighbourhood that = (Neighbourhood) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(population, that.population);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, population);
    }
}
