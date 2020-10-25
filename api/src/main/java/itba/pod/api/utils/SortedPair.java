package itba.pod.api.utils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

public class SortedPair<T extends Comparable<T>> implements Comparable<SortedPair<T>>, Serializable {
    private T a;
    private T b;

    public SortedPair(T a, T b) {
        if (a.compareTo(b) > 0) {
            this.b = a;
            this.a = b;
        } else {
            this.a = a;
            this.b = b;
        }
    }

    @Override
    public int compareTo(SortedPair<T> tSortedPair) {
        int match = a.compareTo(tSortedPair.a);

        if (match != 0) return match;
        return b.compareTo(tSortedPair.b);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortedPair<?> pair = (SortedPair<?>) o;
        return Objects.equals(a, pair.a) &&
                Objects.equals(b, pair.b);
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b);
    }

    public T getA() {
        return a;
    }

    public void setA(T a) {
        this.a = a;
    }

    public T getB() {
        return b;
    }

    public void setB(T b) {
        this.b = b;
    }
}
