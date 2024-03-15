package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class JoinOperator extends Operator {
    private Operator leftChild;
    private Operator rightChild;
    private Expression joinCondition;
    private Tuple currentLeftTuple;

    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinCondition = joinCondition;
        this.currentLeftTuple = null;
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
                Tuple combinedTuple = combineTuples(currentLeftTuple, rightTuple); // Use combineTuples method
                if (joinCondition == null || evaluateJoinCondition(combinedTuple)) {
                    // If there's no join condition or the join condition is satisfied
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

    private boolean evaluateJoinCondition(Tuple combinedTuple) {
        // This method needs to evaluate the join condition using the combined tuple.
        // You'll need to adapt your existing expression evaluation mechanism to work here.
        // This could involve using your SelectEvaluator or similar, with modifications
        // to evaluate the expression against the combined tuple.
        return true; // Placeholder, implement based on your system's capabilities
    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        currentLeftTuple = null;
    }
}
