package ed.inf.adbs.lightdb;

import java.util.*;

public class DuplicateEliminationOperator extends Operator {
    private Operator childOperator;
    private Set<String> seenTupleKeys;
    private Tuple nextUniqueTuple;
    private List<Integer> columnIndexes;

    public DuplicateEliminationOperator(Operator childOperator, Schema schema, List<String> columnNames) {
        this.childOperator = childOperator;
        this.seenTupleKeys = new HashSet<>();
        this.columnIndexes = new ArrayList<>();

        // Initialize columnIndexes based on provided columnNames
        for (String columnName : columnNames) {
            this.columnIndexes.add(schema.getColumnIndex(columnName));
        }

        findNextUniqueTuple();
    }

    private void findNextUniqueTuple() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            String tupleKey = generateTupleKey(tuple);
            if (seenTupleKeys.add(tupleKey)) {
                nextUniqueTuple = tuple;
                return;
            }
        }
        nextUniqueTuple = null; // Signifies that there are no more unique tuples
    }

    // Generates a unique key for a tuple based on the values of the specified columns
    private String generateTupleKey(Tuple tuple) {
        StringBuilder keyBuilder = new StringBuilder();
        for (int index : columnIndexes) {
            keyBuilder.append(tuple.getValueByPos(index)).append("|"); // Use "|" as a delimiter
        }
        return keyBuilder.toString();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple currentTuple = nextUniqueTuple;
        if (currentTuple != null) {
            findNextUniqueTuple(); // Set up for the next call
        }
        return currentTuple;
    }

    @Override
    public void reset() {
    }
}
