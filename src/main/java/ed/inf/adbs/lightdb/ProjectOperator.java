package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator {
    private final Operator childOperator;
    private final List<String> projectionColumns;
    private final Schema schema;

    public ProjectOperator(Operator childOperator, Schema schema, List<String> projectionColumns) {
        this.childOperator = childOperator;
        this.projectionColumns = projectionColumns;
        this.schema = schema; // Initialize schema
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = childOperator.getNextTuple();
        if (tuple == null) return null;

        List<Integer> projectedValues = new ArrayList<>();
        for (String column : projectionColumns) {
            Integer columnIndex = schema.getColumnIndex(column);
            if (columnIndex != null) {
                projectedValues.add(tuple.getValues().get(columnIndex));
            } else {
                throw new RuntimeException("Column not found in schema: " + column);
            }
        }
        return new Tuple(projectedValues);
    }

    @Override
    public void reset() {
        childOperator.reset();
    }
}
