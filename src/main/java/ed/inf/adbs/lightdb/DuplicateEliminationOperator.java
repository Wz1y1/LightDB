package ed.inf.adbs.lightdb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An operator to eliminate duplicate tuples based on specified column names.
 * It ensures that only unique tuples, considering the specified columns, are passed through.
 */
public class DuplicateEliminationOperator extends Operator {
    private final Operator childOperator;
    private final Set<String> seenTupleKeys; // Tracks unique combinations of column values seen so far.
    private final List<Integer> columnIndexes; // Indexes of the columns to consider for duplication check.
    private Tuple nextUniqueTuple; // The next unique tuple to be returned.

    /**
     * Constructs a DuplicateEliminationOperator.
     *
     * @param childOperator The source operator providing tuples to deduplicate.
     * @param schema        The schema to which the tuples conform.
     * @param columnNames   The names of the columns to consider for determining uniqueness.
     */
    public DuplicateEliminationOperator(Operator childOperator, Schema schema, List<String> columnNames) {
        this.childOperator = childOperator;
        this.seenTupleKeys = new HashSet<>();
        // Map column names to their indexes in the schema for faster access during tuple processing.
        this.columnIndexes = columnNames.stream()
                .map(schema::getIndex)
                .collect(Collectors.toList());
        this.nextUniqueTuple = findNextUniqueTuple();
    }

    /**
     * Identifies the next unique tuple based on the specified column indexes.
     *
     * @return The next unique tuple, or null if no more unique tuples are found.
     */
    private Tuple findNextUniqueTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            String tupleKey = generateTupleKey(tuple); // Generate a unique key for the tuple.
            if (seenTupleKeys.add(tupleKey)) {
                return tuple; // If the tuple key is new, return the tuple.
            }
        }
        return null; // Indicates no more unique tuples are found
    }

    /**
     * Generates a unique key for a tuple based on the values of specified columns.
     *
     * @param tuple The tuple for which to generate the key.
     * @return A string representing the unique key for the tuple.
     */
    private String generateTupleKey(Tuple tuple) {
        // Concatenate the values of specified columns to form the key.
        return columnIndexes.stream()
                .map(index -> String.valueOf(tuple.getValueByPos(index)))
                .collect(Collectors.joining("|"));
    }


    /**
     * Returns the next unique tuple or null if no more unique tuples exist.
     *
     * @return Next unique tuple or null.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple currentTuple = this.nextUniqueTuple;
        this.nextUniqueTuple = findNextUniqueTuple(); // Find the next unique tuple for subsequent calls.
        return currentTuple;
    }

    /**
     * Resets the operator, clearing seen tuples and re-evaluating uniqueness.
     */
    @Override
    public void reset() {
        childOperator.reset();
        seenTupleKeys.clear();
        this.nextUniqueTuple = findNextUniqueTuple();  // Re-initialize the next unique tuple.
    }
}
