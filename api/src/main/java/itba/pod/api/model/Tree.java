package itba.pod.api.model;

public class Tree implements CSVEntry {

    private String neighbourhood;
    private String street;
    private String scientificName;
    private Double diameter;

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
}
