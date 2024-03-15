package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;

public class SelectEvaluator extends ExpressionDeParser {
    private final Tuple tuple;
    private final Schema schema;
    private boolean result;

    public SelectEvaluator(Tuple tuple, Schema schema) {
        this.tuple = tuple;
        this.schema = schema;
    }

    private void evaluateBinaryExpression(Expression leftExpression, Expression rightExpression, BinaryOperator<Double> operator) {
        leftExpression.accept(this);
        double leftValue = Double.parseDouble(super.getBuffer().toString());
        super.getBuffer().setLength(0); // Clear the buffer

        rightExpression.accept(this);
        double rightValue = Double.parseDouble(super.getBuffer().toString());
        super.getBuffer().setLength(0); // Again, clear the buffer

        this.result = operator.apply(leftValue, rightValue);
    }

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
        // Evaluate the left side of the AND expression
        andExpression.getLeftExpression().accept(this);
        boolean leftResult = this.result;

        // Short-circuit: if the left result is false, no need to evaluate the right side
        if (!leftResult) {
            this.result = false;
            return;
        }

        // Only evaluate the right side if the left side is true
        andExpression.getRightExpression().accept(this);
        boolean rightResult = this.result;

        // The final result is true only if both sides are true
        this.result = rightResult; // leftResult is already known to be true here
    }

    @Override
    public void visit(OrExpression orExpression) {
        // Evaluate the left side of the OR expression
        orExpression.getLeftExpression().accept(this);
        boolean leftResult = this.result;

        // Short-circuit: if the left result is true, no need to evaluate the right side
        if (leftResult) {
            this.result = true;
            return;
        }

        // Only evaluate the right side if the left side is false
        orExpression.getRightExpression().accept(this);
        boolean rightResult = this.result;

        // The final result is true if either side is true
        this.result = rightResult; // Since leftResult is false, result depends on rightResult
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


    public boolean getResult() {
        return result;
    }

}
