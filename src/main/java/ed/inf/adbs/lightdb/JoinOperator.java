package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class JoinOperator extends Operator {
    private Operator leftChild;
    private Operator rightChild;
    private Tuple leftTuple;
    private Tuple rightTuple;
    private JoinEvalExpression joinEvalExpression;

    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition, Schema combinedSchema) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftTuple = leftChild.getNextTuple();
        this.rightTuple = rightChild.getNextTuple();
        this.joinEvalExpression = new JoinEvalExpression(combinedSchema, joinCondition);
    }

    @Override
    public Tuple getNextTuple() {
        try {
            while (leftTuple != null) {
                if (rightTuple == null) {
                    leftTuple = leftChild.getNextTuple();
                    rightChild.reset();
                    rightTuple = rightChild.getNextTuple();
                    continue;
                }

                if (joinEvalExpression.evaluate(leftTuple, rightTuple)) {
                    Tuple combinedTuple = combineTuples(leftTuple, rightTuple);
                    rightTuple = rightChild.getNextTuple(); // Move to the next right tuple
                    if (combinedTuple != null) return combinedTuple;
                } else {
                    rightTuple = rightChild.getNextTuple(); // Try next right tuple
                }
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return null; // No more tuples match
    }

    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        List<Integer> leftValues = leftTuple.getValues();
        List<Integer> rightValues = rightTuple.getValues();

        // Create a new list to hold combined values
        List<Integer> combinedValues = new ArrayList<>(leftValues);
        combinedValues.addAll(rightValues); // Add all values from the right tuple

        return new Tuple(combinedValues); // Return a new tuple with combined values
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}