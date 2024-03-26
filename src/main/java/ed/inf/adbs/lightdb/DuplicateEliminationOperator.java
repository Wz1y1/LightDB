package ed.inf.adbs.lightdb;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DuplicateEliminationOperator extends Operator {
    private final Operator childOperator;
    private final Set<String> seenTupleKeys;
    private final List<Integer> columnIndexes;
    private Tuple nextUniqueTuple;

    public DuplicateEliminationOperator(Operator childOperator, Schema schema, List<String> columnNames) {
        this.childOperator = childOperator;
        this.seenTupleKeys = new HashSet<>();

        // Directly convert column names to indexes using stream for more concise code
        this.columnIndexes = columnNames.stream()
                .map(schema::getColumnIndex)
                .collect(Collectors.toList());

        this.nextUniqueTuple = findNextUniqueTuple();
    }

    private Tuple findNextUniqueTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            String tupleKey = generateTupleKey(tuple);
            if (seenTupleKeys.add(tupleKey)) {
                return tuple;
            }
        }
        return null; // Indicates no more unique tuples are found
    }

    private String generateTupleKey(Tuple tuple) {
        return columnIndexes.stream()
                .map(index -> String.valueOf(tuple.getValueByPos(index))) // Explicitly convert to String
                .collect(Collectors.joining("|"));
    }


    @Override
    public Tuple getNextTuple() {
        Tuple currentTuple = this.nextUniqueTuple;
        this.nextUniqueTuple = findNextUniqueTuple(); // Pre-fetch the next unique tuple for subsequent calls
        return currentTuple;
    }

    @Override
    public void reset() {
        childOperator.reset();
        seenTupleKeys.clear();
        this.nextUniqueTuple = findNextUniqueTuple(); // Reset state and pre-fetch the first unique tuple again
    }
}
