package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProjectOperator extends Operator {
    private final Operator childOperator;
    private final List<SelectItem<?>> projectionColumns;
    private final Schema schema;

    public ProjectOperator(Operator childOperator, Schema schema, List<SelectItem<?>> projectionColumns) {
        this.childOperator = childOperator;
        this.projectionColumns = projectionColumns;
        this.schema = schema; // Initialize schema
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = childOperator.getNextTuple();
        if (tuple == null) {
            return null;
        }

        List<Integer> selectedValues = new ArrayList<>();
        boolean sumEncountered = false;
        for (SelectItem<?> item : projectionColumns) {
            Expression expr = item.getExpression();
            if (expr instanceof Column && !sumEncountered) {
                int index = schema.getColumnIndex(expr.toString());
                if (index != -1) {
                    selectedValues.add(tuple.getValues().get(index));
                }
            } else if (expr instanceof Function) {
                // Assuming this is your SUM at the end
                sumEncountered = true;
                // Handle SUM calculation if needed, or assume it's already calculated in the tuple
                // For simplicity, this assumes the sum result is already the last value in the tuple
                selectedValues.add(tuple.getValues().get(tuple.getValues().size() - 1));
            }
        }
        return new Tuple(selectedValues);
    }



    @Override
    public void reset() {
        childOperator.reset();
    }
}
