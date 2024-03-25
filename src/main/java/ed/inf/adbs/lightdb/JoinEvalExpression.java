//package ed.inf.adbs.lightdb;
//
//import net.sf.jsqlparser.JSQLParserException;
//import net.sf.jsqlparser.expression.*;
//import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
//import net.sf.jsqlparser.expression.operators.relational.*;
//import net.sf.jsqlparser.parser.CCJSqlParserUtil;
//import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
//
///**
// * Class used to evaluate if 2 tuples fulfill the conditions of a join expression
// * without using Stack for expression evaluation.
// */
//public class JoinEvalExpression {
//    private Tuple t;
//    private Tuple t2;
//    private Expression e;
//    private Schema schema;
//
//    public JoinEvalExpression(Tuple leftTuple, Tuple rightTuple, Expression e, Schema schema) {
//        this.t = leftTuple;
//        this.t2 = rightTuple;
//        this.e = e;
//        this.schema = schema;
//    }
//
//    public Object evaluate() throws JSQLParserException {
//        Expression parsedExpression = CCJSqlParserUtil.parseCondExpression(e.toString());
//        return evaluateExpression(parsedExpression);
//    }
//
//    private Object evaluateExpression(Expression expression) {
//        if (expression instanceof LongValue) {
//            return ((LongValue) expression).getValue();
//        } else if (expression instanceof Column) {
//            return evaluateColumn((Column) expression);
//        } else if (expression instanceof BinaryExpression) {
//            return evaluateBinaryExpression((BinaryExpression) expression);
//        } else if (expression instanceof AndExpression) {
//            return evaluateAndExpression((AndExpression) expression);
//        } else {
//            throw new IllegalArgumentException("Unsupported expression type: " + expression.getClass());
//        }
//    }
//
//    private Object evaluateColumn(Column column) {
//        String fullyQualifiedName = column.getFullyQualifiedName();
//        Integer index = schema.getColumnIndex(fullyQualifiedName);
//        if (index != null) {
//            if (index < t.getValues().size()) {
//                return t.getValueByPos(index);
//            } else {
//                return t2.getValueByPos(index - t.getValues().size());
//            }
//        } else {
//            System.err.println("Column not found: " + fullyQualifiedName);
//            return null; // Or throw an exception, based on your error handling strategy.
//        }
//    }
//
//    private Object evaluateBinaryExpression(BinaryExpression binaryExpression) {
//        Object left = evaluateExpression(binaryExpression.getLeftExpression());
//        Object right = evaluateExpression(binaryExpression.getRightExpression());
//        if (binaryExpression instanceof EqualsTo) {
//            return left.equals(right);
//        } else if (binaryExpression instanceof GreaterThan) {
//            return (long) left > (long) right;
//        } else if (binaryExpression instanceof GreaterThanEquals) {
//            return (long) left >= (long) right;
//        } else if (binaryExpression instanceof MinorThan) {
//            return (long) left < (long) right;
//        } else if (binaryExpression instanceof MinorThanEquals) {
//            return (long) left <= (long) right;
//        } else if (binaryExpression instanceof NotEqualsTo) {
//            return !left.equals(right);
//        } else {
//            throw new IllegalArgumentException("Unsupported binary expression type: " + binaryExpression.getClass());
//        }
//    }
//
//    private Object evaluateAndExpression(AndExpression andExpression) {
//        boolean left = (boolean) evaluateExpression(andExpression.getLeftExpression());
//        boolean right = (boolean) evaluateExpression(andExpression.getRightExpression());
//        return left && right;
//    }
//}

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

