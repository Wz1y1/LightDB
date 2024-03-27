package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Sorts tuples from a child operator based on a specified comparator.
 */
public class SortOperator extends Operator {
    private final Operator childOperator;
    private final List<Tuple> sortedTuples;
    private final Comparator<Tuple> comparator;
    private int currentPo;

    /**
     * Initializes a new SortOperator.
     *
     * @param childOperator The child operator providing tuples to sort.
     * @param comparator    The comparator to determine the order of tuples.
     */
    public SortOperator(Operator childOperator, Comparator<Tuple> comparator) {
        this.childOperator = childOperator;
        this.comparator = comparator;
        this.sortedTuples = new ArrayList<>();
        loadAndSortTuples(); // Load and sort all tuples at initialization.
    }

    /**
     * Loads and sorts tuples from the child operator.
     */
    private void loadAndSortTuples() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            sortedTuples.add(tuple); // Add tuples to the list.
        }
        sortedTuples.sort(comparator); // Sort the list using the comparator.
    }

    /**
     * Returns the next tuple in sorted order, or null if there are no more tuples.
     *
     * @return The next sorted tuple, or null.
     */
    @Override
    public Tuple getNextTuple() {
        if (currentPo < sortedTuples.size()) {
            return sortedTuples.get(currentPo++);
        }
        return null;
    }

    /**
     * Resets the operator to start returning tuples from the beginning.
     */
    @Override
    public void reset() {
        currentPo = 0;
    }
}
