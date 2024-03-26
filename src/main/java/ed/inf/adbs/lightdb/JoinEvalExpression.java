package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

/**
 * Class for evaluating join conditions between two tuples without using a stack.
 */
public class JoinEvalExpression {
    private Schema schema;
    private Expression joinCondition;

    public JoinEvalExpression(Schema schema, Expression joinCondition) {
        this.schema = schema;
        this.joinCondition = joinCondition;
    }

    public boolean evaluate(Tuple leftTuple, Tuple rightTuple) throws JSQLParserException {
        if (joinCondition == null) {
            return true; // Cartesian product, no condition to evaluate.
        }
        Expression parsedExpression = CCJSqlParserUtil.parseCondExpression(joinCondition.toString());
        Object result = evaluateExpression(parsedExpression, leftTuple, rightTuple);
        if (result instanceof Boolean) {
            return (Boolean) result;
        } else {
            throw new RuntimeException("Join condition evaluation did not result in a boolean value.");
        }
    }

    private Object evaluateExpression(Expression expression, Tuple leftTuple, Tuple rightTuple) {
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        } else if (expression instanceof Column) {
            return evaluateColumn((Column) expression, leftTuple, rightTuple);
        } else if (expression instanceof BinaryExpression) {
            return evaluateBinaryExpression((BinaryExpression) expression, leftTuple, rightTuple);
        } else if (expression instanceof AndExpression) {
            return evaluateAndExpression((AndExpression) expression, leftTuple, rightTuple);
        } else {
            throw new IllegalArgumentException("Unsupported expression type: " + expression.getClass().getSimpleName());
        }
    }

    private Object evaluateColumn(Column column, Tuple leftTuple, Tuple rightTuple) {
        String columnName = column.getColumnName();
        Integer index = schema.getColumnIndex(column.getFullyQualifiedName());
        if (index == null) {
            throw new IllegalArgumentException("Column not found in schema: " + columnName);
        }
        // Assuming schema combines both tuples' schemas, with left tuple's columns coming first.
        if (index < leftTuple.getValues().size()) {
            return leftTuple.getValueByPos(index);
        } else {
            return rightTuple.getValueByPos(index - leftTuple.getValues().size());
        }
    }

    private Object evaluateBinaryExpression(BinaryExpression expression, Tuple leftTuple, Tuple rightTuple) {
        Object leftVal = evaluateExpression(expression.getLeftExpression(), leftTuple, rightTuple);
        Object rightVal = evaluateExpression(expression.getRightExpression(), leftTuple, rightTuple);

        if (expression instanceof EqualsTo) {
            return leftVal.equals(rightVal);
        } else if (expression instanceof GreaterThan) {
            return ((Comparable) leftVal).compareTo(rightVal) > 0;
        } else if (expression instanceof GreaterThanEquals) {
            return ((Comparable) leftVal).compareTo(rightVal) >= 0;
        } else if (expression instanceof MinorThan) {
            return ((Comparable) leftVal).compareTo(rightVal) < 0;
        } else if (expression instanceof MinorThanEquals) {
            return ((Comparable) leftVal).compareTo(rightVal) <= 0;
        } else if (expression instanceof NotEqualsTo) {
            return !leftVal.equals(rightVal);
        } else {
            throw new IllegalArgumentException("Unsupported binary expression type: " + expression.getClass().getSimpleName());
        }
    }

    private Object evaluateAndExpression(AndExpression expression, Tuple leftTuple, Tuple rightTuple) {
        boolean leftResult = (boolean) evaluateExpression(expression.getLeftExpression(), leftTuple, rightTuple);
        boolean rightResult = (boolean) evaluateExpression(expression.getRightExpression(), leftTuple, rightTuple);
        return leftResult && rightResult;
    }
}