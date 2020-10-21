package itba.pod.api.model;

public class Neighbourhood implements CSVEntry {

    private String name;
    private Integer population;

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
}
