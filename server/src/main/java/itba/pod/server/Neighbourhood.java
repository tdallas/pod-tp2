package itba.pod.server;

public class Neighbourhood implements CSVEntry {

    // Neighbourhood fields (standard name)
    public static final String NAME = "NAME";
    public static final String POPULATION = "POPULATION";

    private final String name;
    private final Integer population;

    public Neighbourhood(String name, Integer population) {
        this.name = name;
        this.population = population;
    }

    @Override
    public String toString() {
        return String.join(", ", name, population.toString());
    }
}
