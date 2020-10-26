package itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import itba.pod.api.model.Tree;

public class NeighbourhoodCountMapper implements Mapper<String, Tree, String, Long> {
    private final String species;

    public NeighbourhoodCountMapper() {
        // species can't be Optional due to Optional not being DataSerializable
        this.species = "";
    }

    public NeighbourhoodCountMapper(String species) {
        this.species = species;
    }

    @Override
    public void map(String s, Tree tree, Context<String, Long> context) {
        if (species.isEmpty() || tree.getScientificName().equals(species))
            context.emit(tree.getNeighbourhood(), 1L);
    }
}
