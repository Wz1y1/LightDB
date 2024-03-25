package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SumOperator extends Operator {
    private final Operator child;
    private final String columnName;
    private final List<String> groupByColumns;
    private final Map<List<Object>, Long> groupSums;
    private final List<Tuple> results;
    private int currentIndex;
    private final Schema schema;

    public SumOperator(Operator child, String columnName, List<String> groupByColumns, Schema schema) {
        this.child = child;
        this.columnName = columnName;
        this.groupByColumns = new ArrayList<>(groupByColumns);
        this.schema = schema;
        this.groupSums = new HashMap<>();
        this.results = new ArrayList<>();
        this.currentIndex = 0;
        performAggregation();
    }

    private void performAggregation() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            List<Object> groupKey = generateGroupKey(tuple);
            long valueToSum = fetchValueToSum(columnName, tuple);
            groupSums.merge(groupKey, valueToSum, Long::sum);
        }

        generateResults();
    }

    private List<Object> generateGroupKey(Tuple tuple) {
        return groupByColumns.stream()
                .map(col -> tuple.getValueByPos(schema.getColumnIndex(col)))
                .collect(Collectors.toList());
    }

    private long fetchValueToSum(String colName, Tuple tuple) {
        if (colName == null) return 0;

        if (isNumericLiteral(colName)) {
            return Long.parseLong(colName);
        }
        // Assuming isProductExpression handles checking for product expressions
        return isProductExpression(colName) ? evaluateProductExpression(colName, tuple)
                : tuple.getValueByPos(schema.getColumnIndex(colName));
    }

    private boolean isNumericLiteral(String str) {
        // Return false immediately if str is null
        if (str == null) return false;
        return str.matches("-?\\d+");
    }

    private boolean isProductExpression(String expression) {
        return expression.contains("*");
    }

    private long evaluateProductExpression(String expression, Tuple tuple) {
        String[] parts = expression.split("\\*");
        long product = 1;
        for (String part : parts) {
            int index = schema.getColumnIndex(part.trim());
            product *= tuple.getValueByPos(index);
        }
        return product;
    }

    private void generateResults() {
        for (Map.Entry<List<Object>, Long> entry : groupSums.entrySet()) {
            List<Integer> tupleValues = entry.getKey().stream()
                    .map(Object::toString)
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
            tupleValues.add(entry.getValue().intValue()); // Convert sum back to Integer for Tuple
            results.add(new Tuple(tupleValues));
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (currentIndex >= results.size()) return null;
        return results.get(currentIndex++);
    }

    @Override
    public void reset() {
        currentIndex = 0;
    }

    public Schema getSchema() {
        // Assuming Schema construction is correct for your context
        List<String> columnNames = new ArrayList<>(groupByColumns);
        columnNames.add("Sum");
        return new Schema("Aggregated", columnNames);
    }
}
