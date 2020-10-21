package itba.pod.server;

public class Tree implements CSVEntry {

    // Tree fields (standard name)
    public static final String NEIGHBOURHOOD = "NEIGHBOURHOOD";
    public static final String STREET = "STREET";
    public static final String SCIENTIFIC_NAME = "SCIENTIFIC_NAME";
    public static final String DIAMETER = "DIAMETER";

    private final String neighbourhood;
    private final String street;
    private final String scientificName;
    private final Double diameter;

    public Tree(String neighbourhood, String street, String scientificName, Double diameter) {
        this.neighbourhood = neighbourhood;
        this.street = street;
        this.scientificName = scientificName;
        this.diameter = diameter;
    }

    @Override
    public String toString() {
        return String.join(", ", neighbourhood, street, scientificName, diameter.toString());
    }
}
