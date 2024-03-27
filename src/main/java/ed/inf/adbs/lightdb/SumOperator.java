package ed.inf.adbs.lightdb;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The SumOperator class aggregates tuples from its child operator based on a sum aggregation function.
 * It supports grouping results by one or more columns and calculates the sum of values for a specified column within each group.
 */
public class SumOperator extends Operator {
    private final Operator childOperator;
    private final String columnName;
    private final List<String> groupByColumns;
    private final Map<String, Long> groupSums = new HashMap<>();
    private final List<Tuple> results = new ArrayList<>();
    private int currentIndex = 0;
    private final Schema schema;

    /**
     * Creates an instance of SumOperator.
     *
     * @param child          The child operator from which tuples are fetched.
     * @param columnName     The name of the column to sum up.
     * @param groupByColumns The list of column names for grouping the sum.
     * @param schema         The schema used for resolving column names.
     */
    public SumOperator(Operator child, String columnName, List<String> groupByColumns, Schema schema) {
        this.childOperator = child;
        this.columnName = columnName;
        this.groupByColumns = new ArrayList<>(groupByColumns);
        this.schema = schema;
        performAggregation();
    }

    /**
     * Aggregates the sum of specified column values, grouping by the specified columns.
     */
    private void performAggregation() {
        Tuple tuple;
        while ((tuple = childOperator.getNextTuple()) != null) {
            String groupKey = generateGroupKey(tuple);
            long valueToSum = fetchValueToSum(columnName, tuple);
            groupSums.merge(groupKey, valueToSum, Long::sum);
        }
        generateResults();
    }

    /**
     * Generates a group key for a tuple based on the groupByColumns.
     *
     * @param tuple The tuple from which to generate the group key.
     * @return A concatenated string representing the group key.
     */
    private String generateGroupKey(Tuple tuple) {
        return groupByColumns.stream()
                .map(col -> {
                    Object value = tuple.getValueByPos(schema.getIndex(col));
                    return value.toString();
                })
                .collect(Collectors.joining("_"));
    }


    /**
     * Fetches a numeric value from a given tuple based on the specified expression.
     * The method handles different types of expressions including:
     * - Numeric literals, directly returning their value.
     * - Product expressions, involving multiplication of values from multiple columns.
     * - Regular column references, extracting the value from the specified column.
     *
     * @param expression The expression indicating what value to sum, which can be a numeric literal,
     *                   a product of column values (e.g., "column1 * column2"), or a single column name.
     * @param tuple      The tuple from which the value is fetched.
     * @return The numeric value indicated by the expression from the tuple. If the expression is invalid,
     *         a numeric literal, or if the specified column value cannot be converted to a long, returns 0.
     *         If the expression is a product expression and any part of the product cannot be evaluated,
     *         also returns 0.
     */
    private long fetchValueToSum(String expression, Tuple tuple) {
        // Check if the expression is null or empty, defaulting to 0 for the sum value.
        if (expression == null || expression.isEmpty()) {
            return 0;
        }

        // Directly parse and return the value if the expression is a numeric literal.
        if (expression.matches("-?\\d+(\\.\\d+)?")) {
            return Long.parseLong(expression);
        }

        // Evaluate the expression as a product of column values if it contains a multiplication symbol.
        if (expression.contains("*")) {
            return evaluateProductExpression(expression, tuple);
        }

        // Attempt to fetch the value from the tuple based on column name for regular column references.
        int index = schema.getIndex(expression);
        if (index >= 0 && index < tuple.getSize()) {
            Object value = tuple.getValueByPos(index);
            // Attempt to parse the column value as a long.
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
                // Log or handle the exception as appropriate for your application.
                System.err.println("Error parsing column value as long: " + e.getMessage());
            }
        }

        // Return 0 as a fallback for any cases where the value cannot be determined.
        return 0;
    }


    /**
     * Evaluates a product expression contained within a tuple.
     *
     * @param expression The product expression to evaluate.
     * @param tuple      The tuple containing the values for the expression.
     * @return The result of the product expression.
     */
    private long evaluateProductExpression(String expression, Tuple tuple) {
        return Arrays.stream(expression.split("\\*"))
                .map(String::trim)
                .mapToLong(part -> {
                    int index = schema.getIndex(part);
                    return tuple.getValueByPos(index);
                })
                .reduce(1, (a, b) -> a * b);
    }

    /**
     * Generates aggregated results as tuples. Handles both global aggregation (no group-by columns)
     * and grouped aggregation (with group-by columns).
     */
    private void generateResults() {
        if (groupByColumns.isEmpty()) {
            // Global aggregation: Sum all values.
            long globalSum = groupSums.values().stream().mapToLong(Long::longValue).sum();
            results.add(new Tuple(Collections.singletonList((int) globalSum)));
        } else {
            // Grouped aggregation: Create a tuple for each group's sum.
            groupSums.forEach((key, sum) -> {
                List<Integer> values = Arrays.stream(key.split("_"))
                        .filter(s -> !s.isEmpty())
                        .map(Integer::parseInt)
                        .collect(Collectors.toList());
                values.add(sum.intValue());
                results.add(new Tuple(values));
            });
        }
    }


    /**
     * Gets the next aggregated tuple or null if all have been returned.
     *
     * @return Next aggregated tuple or null.
     */
    @Override
    public Tuple getNextTuple() {
        return currentIndex < results.size() ? results.get(currentIndex++) : null;
    }

    /**
     * Resets the operator, allowing tuples to be iterated again from the start.
     */
    @Override
    public void reset() {
        currentIndex = 0;
    }

    /**
     * Returns the schema for the aggregated results, including group by columns and the sum.
     *
     * @return Schema of the results.
     */
    public Schema getSchema() {
        List<String> columnNames = new ArrayList<>(groupByColumns);
        columnNames.add("Sum");
        return new Schema("Aggregated", columnNames);
    }
}
