package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class JoinEvalExpression {
    private final Schema combinedSchema;

    public JoinEvalExpression(Schema combinedSchema) {
        this.combinedSchema = combinedSchema;
    }

    public boolean evaluate(Expression joinCondition, Tuple leftTuple, Tuple rightTuple) {
        CustomExpressionEvaluator evaluator = new CustomExpressionEvaluator(leftTuple, rightTuple, combinedSchema);
        joinCondition.accept(evaluator);
        return evaluator.isConditionMet();
    }

    private static class CustomExpressionEvaluator extends ExpressionDeParser {
        private boolean conditionMet = true;
        private final Tuple leftTuple;
        private final Tuple rightTuple;
        private final Schema combinedSchema;

        public CustomExpressionEvaluator(Tuple leftTuple, Tuple rightTuple, Schema combinedSchema) {
            this.leftTuple = leftTuple;
            this.rightTuple = rightTuple;
            this.combinedSchema = combinedSchema;
        }

        public boolean isConditionMet() {
            return conditionMet;
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            evaluateBinaryExpression(equalsTo);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            evaluateBinaryExpression(greaterThan);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            evaluateBinaryExpression(greaterThanEquals);
        }

        @Override
        public void visit(MinorThan minorThan) {
            evaluateBinaryExpression(minorThan);
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            evaluateBinaryExpression(minorThanEquals);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            evaluateBinaryExpression(notEqualsTo);
        }

        @Override
        public void visit(AndExpression andExpression) {
            andExpression.getLeftExpression().accept(this);
            boolean leftResult = this.conditionMet;

            // Only proceed to evaluate the right expression if the left is true
            if (leftResult) {
                andExpression.getRightExpression().accept(this);
                this.conditionMet = this.conditionMet && leftResult;
            } else {
                this.conditionMet = false;
            }
        }

        // Overriding visit method for OrExpression
        @Override
        public void visit(OrExpression orExpression) {
            orExpression.getLeftExpression().accept(this);
            boolean leftResult = this.conditionMet;

            // If the left result is true, the whole expression is true without evaluating the right side
            if (!leftResult) {
                orExpression.getRightExpression().accept(this);
                this.conditionMet = this.conditionMet || leftResult;
            }
        }

        private void evaluateBinaryExpression(BinaryExpression binaryExpression) {
            Expression leftExpression = binaryExpression.getLeftExpression();
            Expression rightExpression = binaryExpression.getRightExpression();

            // Simplification for demo purposes: assuming all expressions involve columns directly
            String leftColumnName = ((Column) leftExpression).getFullyQualifiedName();
            String rightColumnName = ((Column) rightExpression).getFullyQualifiedName();

            Integer leftIndex = combinedSchema.getColumnIndex(leftColumnName);
            Integer rightIndex = combinedSchema.getColumnIndex(rightColumnName);

            if (leftIndex == null || rightIndex == null) {
                conditionMet = false;
                return;
            }

            // Adjust index for right tuple values
            rightIndex -= leftTuple.getValues().size();

            Integer leftValue = leftTuple.getValues().get(leftIndex);
            Integer rightValue = rightTuple.getValues().get(rightIndex);

            switch (binaryExpression.getClass().getSimpleName()) {
                case "EqualsTo":
                    conditionMet = leftValue.equals(rightValue);
                    break;
                case "GreaterThan":
                    conditionMet = leftValue > rightValue;
                    break;
                case "GreaterThanEquals":
                    conditionMet = leftValue >= rightValue;
                    break;
                case "MinorThan":
                    conditionMet = leftValue < rightValue;
                    break;
                case "MinorThanEquals":
                    conditionMet = leftValue <= rightValue;
                    break;
                case "NotEqualsTo":
                    conditionMet = !leftValue.equals(rightValue);
                    break;
                default:
                    conditionMet = false;
            }
        }
    }
}

