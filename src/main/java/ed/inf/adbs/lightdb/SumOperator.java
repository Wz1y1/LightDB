package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SumOperator is responsible for aggregating tuples from its child operator based on a sum aggregation function.
 * It can optionally group results by one or more columns, summing values of a specified column or expression within each group.
 */
public class SumOperator extends Operator {
    private final Operator child;
    private final String columnName;
    private final List<String> groupByColumns;
    private final Map<List<Object>, Long> groupSums;
    private final List<Tuple> results;
    private int currentIndex;
    private final Schema schema;

    /**
     * Constructs a SumOperator with the given parameters.
     *
     * @param child          The child operator from which tuples are fetched.
     * @param columnName     The name of the column or expression to sum. It can be a numeric literal, a column name, or a product expression.
     * @param groupByColumns A list of column names to group by. The operator aggregates values within each group.
     * @param schema         The schema against which column names are resolved.
     */
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

    /**
     * Performs aggregation of tuples fetched from the child operator. This method populates the groupSums map and results list.
     */
    private void performAggregation() {
        Tuple tuple;
        while ((tuple = child.getNextTuple()) != null) {
            List<Object> groupKey = generateGroupKey(tuple);
            long valueToSum = fetchValueToSum(columnName, tuple);
            groupSums.merge(groupKey, valueToSum, Long::sum);
        }

        generateResults();
    }

    /**
     * Generates a key for grouping tuples based on the values of the groupByColumns.
     *
     * @param tuple The tuple from which to extract the grouping values.
     * @return A list of Objects representing the group key.
     */
    private List<Object> generateGroupKey(Tuple tuple) {
        return groupByColumns.stream()
                .map(col -> tuple.getValueByPos(schema.getColumnIndex(col)))
                .collect(Collectors.toList());
    }


    /**
     * Fetches the value to be summed based on an expression, which can be a numeric literal, a product expression, or a column name.
     *
     * @param expression The expression indicating what to sum.
     * @param tuple      The tuple from which to fetch the value.
     * @return The value to be summed.
     */
    private long fetchValueToSum(String expression, Tuple tuple) {
        // Return 0 if the expression is null or empty.
        if (expression == null || expression.isEmpty()) return 0;

        // Check if the expression is a numeric literal.
        if (expression.matches("-?\\d+")) {
            return Long.parseLong(expression);
        }

        // Check if the expression is a product of columns.
        if (expression.contains("*")) {
            return evaluateProductExpression(expression, tuple);
        }

        // If the expression is a regular column reference, fetch its value from the tuple.
        int index = schema.getColumnIndex(expression);
        if (index < 0) {
            throw new IllegalArgumentException("Column '" + expression + "' not found in schema.");
        }
        return tuple.getValueByPos(index);
    }


    /**
     * Evaluates a product expression, multiplying values from specified columns in a tuple.
     *
     * @param expression The product expression to evaluate.
     * @param tuple      The tuple containing the values.
     * @return The product of the specified values.
     */
    private long evaluateProductExpression(String expression, Tuple tuple) {
        // Split the expression into parts based on the '*' operator.
        String[] parts = expression.split("\\*");
        long product = 1; // Start with a product of 1 since we're multiplying.

        for (String part : parts) {
            // Trim whitespace and fetch the index of the column in the schema.
            int index = schema.getColumnIndex(part.trim());
            if (index < 0) {
                // If the column is not found in the schema, log an error or throw an exception.
                throw new IllegalArgumentException("Column '" + part.trim() + "' not found in schema for product expression evaluation.");
            }

            try {
                // Multiply the current product with the value in the column at the index.
                product *= tuple.getValueByPos(index);
            } catch (IndexOutOfBoundsException | NumberFormatException e) {
                // Log or handle any exceptions caused by accessing invalid index or parsing errors.
                System.err.println("Error evaluating product expression '" + expression + "' for tuple: " + e.getMessage());
                throw e;
            }
        }
        return product;
    }

    /**
     * Generates the final results after aggregation, creating new tuples for each group with their aggregated sums.
     */
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

    /**
     * Fetches the next aggregated tuple. Once all tuples are processed, returns null.
     *
     * @return The next tuple with aggregated sum or null if there are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        if (currentIndex >= results.size()) return null;
        return results.get(currentIndex++);
    }

    /**
     * Resets the operator to its initial state, allowing for re-iteration over the aggregated results.
     */
    @Override
    public void reset() {
        currentIndex = 0; // Resets the index to allow re-iteration from the beginning
    }

    /**
     * Generates and returns a schema for the aggregated results. The schema will include group by columns and a "Sum" column.
     *
     * @return The schema of the aggregated results.
     */
    public Schema getSchema() {
        List<String> columnNames = new ArrayList<>(groupByColumns); // Includes all group by columns in the schema
        columnNames.add("Sum"); // Adds a "Sum" column to represent the aggregated sum
        return new Schema("Aggregated", columnNames); // Returns a new Schema object with the specified column names
    }
}
