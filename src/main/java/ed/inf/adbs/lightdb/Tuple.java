package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Tuple {
    private List<Integer> values;

    public Tuple(List<Integer> values) {
        this.values = new ArrayList<>(values);
    }

    public List<Integer> getValues() {
        return values;
    }

    public int getValueByPos(int i) {
        return values.get(i);
    }




    @Override
    public String toString() {
        // Convert each integer to a string for joining
        return values.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }
}


