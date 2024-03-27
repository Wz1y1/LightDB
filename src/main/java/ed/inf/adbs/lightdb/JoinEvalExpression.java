package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * Evaluates SQL join conditions between tuples from two relations.
 * This class extends {@link ExpressionDeParser} to visit and evaluate different types of expressions.
 */
public class JoinEvalExpression extends ExpressionDeParser {

    private Tuple leftTuple;
    private Tuple rightTuple;
    private final Schema schema;
    private boolean result;
    private Object currentValue;

    /**
     * Constructs a new instance to evaluate join expressions based on the specified schema.
     *
     * @param schema The combined schema of the left and right tuples involved in the join.
     */
    public JoinEvalExpression(Schema schema) {
        this.schema = schema;
    }

    /**
     * Evaluates a join expression against pairs of tuples from two relations.
     *
     * @param expression The join expression to evaluate.
     * @param leftTuple  The tuple from the left relation.
     * @param rightTuple The tuple from the right relation.
     * @return True if the expression evaluates to true for the given pair of tuples, false otherwise.
     * @throws JSQLParserException If an error occurs during expression parsing.
     */
    public boolean evaluate(Expression expression, Tuple leftTuple, Tuple rightTuple) throws JSQLParserException {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        expression.accept(this);
        return result;
    }

    /**
     * Visits a column reference in the expression.
     * Determines whether the column belongs to the left or right tuple and retrieves the corresponding value.
     *
     * @param column The column to visit.
     */
    @Override
    public void visit(Column column) {
        String fullColumnName = column.getFullyQualifiedName();
        int index = schema.getIndex(fullColumnName);

        if (index != -1) {
            if (index < leftTuple.getSize()) {
                currentValue = leftTuple.getValueByPos(index);
            } else {
                int rightIndex = index - leftTuple.getSize();
                currentValue = rightTuple.getValueByPos(rightIndex);
            }
        } else {
            System.out.println("Column not found in schema: " + fullColumnName);
        }
    }


    private Object getCurrentValue() {
        return this.currentValue;
    }

    // Methods for visiting different types of expressions follow. Each method
    // sets the 'result' field based on the evaluation of the expression for the current pair of tuples.

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        Object leftValue = getCurrentValue();
        equalsTo.getRightExpression().accept(this);
        Object rightValue = getCurrentValue();

        result = leftValue.equals(rightValue);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        Object leftValue = getCurrentValue();
        notEqualsTo.getRightExpression().accept(this);
        Object rightValue = getCurrentValue();

        result = !leftValue.equals(rightValue);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        Comparable leftValue = (Comparable) getCurrentValue();
        greaterThan.getRightExpression().accept(this);
        Comparable rightValue = (Comparable) getCurrentValue();

        result = leftValue.compareTo(rightValue) > 0;
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        Comparable leftValue = (Comparable) getCurrentValue();
        minorThan.getRightExpression().accept(this);
        Comparable rightValue = (Comparable) getCurrentValue();

        result = leftValue.compareTo(rightValue) < 0;
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        Comparable leftValue = (Comparable) getCurrentValue();
        greaterThanEquals.getRightExpression().accept(this);
        Comparable rightValue = (Comparable) getCurrentValue();

        result = leftValue.compareTo(rightValue) >= 0;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        Comparable leftValue = (Comparable) getCurrentValue();
        minorThanEquals.getRightExpression().accept(this);
        Comparable rightValue = (Comparable) getCurrentValue();

        result = leftValue.compareTo(rightValue) <= 0;
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftResult = this.result;
        andExpression.getRightExpression().accept(this);
        boolean rightResult = this.result;

        result = leftResult && rightResult;
    }
}
