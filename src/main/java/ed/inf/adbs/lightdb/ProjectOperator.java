package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * The ProjectOperator is responsible for projecting specified columns or applying specific
 * functions like SUM to the tuples fetched by its child operator. This implementation
 * allows for handling both column projection and aggregation functions.
 */
public class ProjectOperator extends Operator {
    private final Operator childOperator; // The child operator supplying the input tuples
    private final List<SelectItem<?>> projectionColumns; // The columns or functions to project
    private final Schema schema; // The schema used for identifying column indexes

    /**
     * Constructs a ProjectOperator.
     *
     * @param childOperator The child operator to fetch input tuples from.
     * @param schema The schema of the tuples provided by the child operator.
     * @param projectionColumns The columns or aggregation functions to project.
     */
    public ProjectOperator(Operator childOperator, Schema schema, List<SelectItem<?>> projectionColumns) {
        this.childOperator = childOperator;
        this.projectionColumns = projectionColumns;
        this.schema = schema; // Initialize schema
    }


    /**
     * Fetches the next tuple from the child operator, applies the projection based on the
     * specified columns or functions, and returns the resulting tuple.
     *
     * @return The next projected tuple, or null if there are no more tuples.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple = childOperator.getNextTuple();
        if (tuple == null) {
            return null;
        }
        List<Integer> selectedValues = new ArrayList<>();
        for (SelectItem item : projectionColumns) {
            Expression expr = item.getExpression();
            if (expr instanceof Column) {
                handleColumnExpression((Column) expr, tuple, selectedValues);
            } else if (expr instanceof Function) {
                handleFunctionExpression((Function) expr, tuple, selectedValues);
            }
        }
        return new Tuple(selectedValues);
    }

    /**
     * Handles the projection of a column expression by adding its value to the list of
     * selected values for the current tuple.
     *
     * @param column The column expression to be handled.
     * @param tuple The current tuple being processed.
     * @param selectedValues The list of selected values for the projected tuple.
     */
    private void handleColumnExpression(Column column, Tuple tuple, List<Integer> selectedValues) {
        int index = schema.getIndex(column.getFullyQualifiedName());
        if (index != -1) {
            selectedValues.add(tuple.getValueByPos(index));
        }
    }

    /**
     * Handles the special case where the expression is a function, such as SUM.
     * It supports adding the last value of the tuple if the function
     * is SUM, simulating an aggregation operation.
     *
     * @param function The function to apply.
     * @param tuple The current tuple being processed.
     * @param selectedValues The list of selected values for the new tuple.
     */
    private void handleFunctionExpression(Function function, Tuple tuple, List<Integer> selectedValues) {
        if (function.getName().equalsIgnoreCase("SUM")) {
            selectedValues.add(tuple.getValues().get(tuple.getValues().size() - 1));
        }
    }


    /**
     * Resets the state of this operator and its child operator, allowing for reiteration
     * over the tuples.
     */
    @Override
    public void reset() {
        childOperator.reset();
    }
}
