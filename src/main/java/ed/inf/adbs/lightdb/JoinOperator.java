package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class JoinOperator extends Operator {
    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;
    private Tuple currentLeftTuple;
    private Schema combinedSchema; // New member for combined schema


    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition, Schema combinedSchema) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
        this.currentLeftTuple = null;
        this.combinedSchema = combinedSchema;

    }

    @Override
    public Tuple getNextTuple() {
        while (true) {
            if (currentLeftTuple == null) {
                currentLeftTuple = leftChild.getNextTuple();
                if (currentLeftTuple == null) return null; // No more tuples in the left child
                rightChild.reset(); // Reset right child for a new left tuple
            }

            Tuple rightTuple;
            while ((rightTuple = rightChild.getNextTuple()) != null) {
                // Instead of immediately combining, first check the join condition with separate tuples
                if (joinCondition == null || evaluateJoinCondition(currentLeftTuple, rightTuple)) {
                    // If there's no join condition or the join condition is satisfied
                    // Now combine the tuples after confirming the condition is satisfied
                    Tuple combinedTuple = combineTuples(currentLeftTuple, rightTuple);
                    return combinedTuple;
                }
            }

            // Finished scanning the right child for the current left tuple, move to the next left tuple
            currentLeftTuple = null;
        }
    }


    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        List<Integer> leftValues = leftTuple.getValues();
        List<Integer> rightValues = rightTuple.getValues();

        // Create a new list to hold combined values
        List<Integer> combinedValues = new ArrayList<>(leftValues);
        combinedValues.addAll(rightValues); // Add all values from the right tuple

        return new Tuple(combinedValues); // Return a new tuple with combined values
    }

    private boolean evaluateJoinCondition(Tuple leftTuple, Tuple rightTuple) {
        JoinEvalExpression evalExpression = new JoinEvalExpression(combinedSchema);
        return evalExpression.evaluate(joinCondition, leftTuple, rightTuple);
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        currentLeftTuple = null;
    }
}
