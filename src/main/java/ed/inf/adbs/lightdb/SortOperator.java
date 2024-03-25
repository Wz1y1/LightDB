package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.Operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortOperator extends Operator {
    private final Operator childOperator;
    private final List<Tuple> sortedTuples;
    private final Comparator<Tuple> comparator;
    private int currentIndex = 0;

    public SortOperator(Operator childOperator, Comparator<Tuple> comparator) {
        this.childOperator = childOperator;
        this.comparator = comparator;
        this.sortedTuples = new ArrayList<>();
        loadAndSortTuples();
    }

    private void loadAndSortTuples() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            sortedTuples.add(tuple);
        }
        Collections.sort(sortedTuples, comparator);
    }

    @Override
    public Tuple getNextTuple() {
        if (currentIndex < sortedTuples.size()) {
            return sortedTuples.get(currentIndex++);
        }
        return null;
    }

    @Override
    public void reset() {
        currentIndex = 0;
    }

    // Implement the dump method if necessary
}
