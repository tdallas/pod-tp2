package itba.pod.server;

public class Neighbourhood implements CSVEntry {

    private String name;
    private Integer population;

    public Neighbourhood(String name, Integer population) {
        this.name = name;
        this.population = population;
    }
}
