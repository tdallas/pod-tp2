package itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import itba.pod.api.model.Tree;

public class MinTreesMapper implements Mapper<String, Tree, String, Long> {
    private final String species;

    public MinTreesMapper(String species) {
        this.species = species;
    }

    @Override
    public void map(String s, Tree tree, Context<String, Long> context) {
        if (tree.getScientificName().equals(species))
            context.emit(tree.getNeighbourhood(), 1L);
    }
}
