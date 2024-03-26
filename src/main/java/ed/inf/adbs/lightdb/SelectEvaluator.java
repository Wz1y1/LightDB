package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;

/**
 * The {@code SelectEvaluator} class extends {@code ExpressionDeParser} to evaluate
 * SQL where condition expressions against tuples.
 * It supports a range of conditional and relational expressions, such as
 * {@code >, >=, <, <=, =, !=}, AND, OR operations.
 */
public class SelectEvaluator extends ExpressionDeParser {
    private final Tuple tuple;
    private final Schema schema;
    private boolean result;

    /**
     * Constructs a {@code SelectEvaluator} for a specific tuple and schema.
     *
     * @param tuple  The tuple to be evaluated against the expression.
     * @param schema The schema defining the structure of the tuple.
     */
    public SelectEvaluator(Tuple tuple, Schema schema) {
        this.tuple = tuple;
        this.schema = schema;
    }

    /**
     * Evaluates a binary expression by applying a specified operation to the left and right operands.
     *
     * @param leftExpression  The left operand as an expression.
     * @param rightExpression The right operand as an expression.
     * @param operator        The operation to apply.
     */
    private void evaluateBinaryExpression(Expression leftExpression, Expression rightExpression, BinaryOperator<Double> operator) {
        leftExpression.accept(this);
        double leftValue = Double.parseDouble(super.getBuffer().toString());
        super.getBuffer().setLength(0); // Clear the buffer

        rightExpression.accept(this);
        double rightValue = Double.parseDouble(super.getBuffer().toString());
        super.getBuffer().setLength(0); // Again, clear the buffer

        this.result = operator.apply(leftValue, rightValue);
    }

    // Overridden visit methods for different types of expressions. Each method evaluates
    // the expression and sets the result field accordingly.
    @Override
    public void visit(GreaterThan greaterThan) {
        evaluateBinaryExpression(greaterThan.getLeftExpression(), greaterThan.getRightExpression(), (left, right) -> left > right);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        evaluateBinaryExpression(greaterThanEquals.getLeftExpression(), greaterThanEquals.getRightExpression(),
                (left, right) -> left >= right);
    }

    @Override
    public void visit(MinorThan minorThan) {
        evaluateBinaryExpression(minorThan.getLeftExpression(), minorThan.getRightExpression(), (left, right) -> left < right);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        evaluateBinaryExpression(minorThanEquals.getLeftExpression(), minorThanEquals.getRightExpression(),
                (left, right) -> left <= right);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        evaluateBinaryExpression(equalsTo.getLeftExpression(), equalsTo.getRightExpression(),
                (left, right) -> Double.compare(left, right) == 0);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        evaluateBinaryExpression(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression(),
                (left, right) -> Double.compare(left, right) != 0);
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftResult = this.result;
        if (!leftResult) {
            return;
        }
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        boolean leftResult = this.result;
        if (leftResult) {
            return;
        }
        orExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(Column column) {
        String fullyQualifiedName = column.getFullyQualifiedName();
        Integer index = schema.getColumnIndex(fullyQualifiedName);

        if (index != null) {
            Object value = tuple.getValues().get(index); // Directly access the integer value
            super.getBuffer().setLength(0); // Clear any previous value
            super.getBuffer().append(value.toString()); // Convert integer to String for compatibility
        } else {
            throw new RuntimeException("Column not found: " + fullyQualifiedName);
        }
    }

    @FunctionalInterface
    interface BinaryOperator<T> {
        boolean apply(T left, T right);
    }

    /**
     * Returns the result of the evaluation.
     *
     * @return {@code true} if the tuple satisfies the where condition, {@code false} otherwise.
     */
    public boolean getResult() {
        return result;
    }
}
