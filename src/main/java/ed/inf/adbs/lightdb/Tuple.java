package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Tuple {
    private final List<Integer> values;

    public Tuple(List<Integer> values) {
        this.values = new ArrayList<>(values);
    }

    public List<Integer> getValues() {
        return new ArrayList<>(values);
    }

    public int getValueByPos(int index) {
        return values.get(index);
    }

    @Override
    public String toString() {
        return values.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
    }

    // Adding comparison capability within the Tuple class itself.
    // This method compares this tuple to another tuple based on a specified column index.
    public int compareTo(Tuple other, int columnIndex) {
        if (other == null || columnIndex < 0 || columnIndex >= this.values.size() || columnIndex >= other.values.size()) {
            throw new IllegalArgumentException("Invalid comparison argument(s)");
        }
        return Integer.compare(this.values.get(columnIndex), other.values.get(columnIndex));
    }
}
