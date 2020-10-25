package itba.pod.api.model;

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

    public String getName() {
        return name;
    }

    public Integer getPopulation() {
        return population;
    }

    @Override
    public String toString() {
        return String.join(", ", name, population.toString());
    }
}
