package itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import itba.pod.api.model.Tree;

public class DiameterPerSpeciesMapper implements Mapper<String, Tree, String, Double> {

    @Override
    public void map(String key, Tree tree, Context<String, Double> context) {
        context.emit(tree.getScientificName(), tree.getDiameter());
    }
}
