package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;
/**
 * Implements a join operator that combines tuples from two child operators based on join conditions.
 * This operator supports natural join and cross join operations.
 */
public class JoinOperator extends Operator {
    private final Operator leftChild;
    private final Operator rightChild;
    private Tuple leftTuple;
    private Tuple rightTuple;
    private final JoinEvalExpression joinEvalExpression;
    private final List<Expression> joinConditions;

    /**
     * Constructs a JoinOperator with specified left and right child operators, join conditions, and a combined schema.
     *
     * @param leftChild The left child operator.
     * @param rightChild The right child operator.
     * @param joinConditions A list of expressions representing the join conditions.
     * @param combinedSchema The schema combining attributes of both left and right child operators.
     */
    public JoinOperator(Operator leftChild, Operator rightChild, List<Expression> joinConditions, Schema combinedSchema) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinConditions = joinConditions;
        this.leftTuple = leftChild.getNextTuple();
        this.rightTuple = rightChild.getNextTuple();
        this.joinEvalExpression = new JoinEvalExpression(combinedSchema);
    }

    /**
     * Retrieves the next tuple that satisfies the join conditions from the child operators.
     * This method implements the logic for both natural join and cross join based on the provided join conditions.
     *
     * @return The next tuple that satisfies the join conditions, or null if no more matching tuples exist.
     */
    @Override
    public Tuple getNextTuple() {
        try {
            while (leftTuple != null) {
                while (rightTuple != null) {
                    boolean conditionsMet = true;

                    // Evaluate each join condition against the current pair of tuples
                    if (joinConditions != null) {
                        for (Expression condition : joinConditions) {
                            if (!joinEvalExpression.evaluate(condition, leftTuple, rightTuple)) {
                                conditionsMet = false;
                                break;
                            }
                        }
                    }
                    // If joinConditions is null, assume a cross join
                    if (conditionsMet) {
                        Tuple combinedTuple = combineTuples(leftTuple, rightTuple);
                        rightTuple = rightChild.getNextTuple(); // Move to the next right tuple
                        return combinedTuple;
                    } else {
                        rightTuple = rightChild.getNextTuple(); // Try next right tuple
                    }
                }

                // Prepare for the next left tuple and reset the right child to its first tuple
                leftTuple = leftChild.getNextTuple();
                rightChild.reset();
                rightTuple = rightChild.getNextTuple();
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return null; // No more tuples match
    }


    /**
     * Combines the values of two tuples into a single tuple.
     *
     * @param leftTuple The tuple from the left child operator.
     * @param rightTuple The tuple from the right child operator.
     * @return A new tuple combining the values from both input tuples.
     */
    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
        List<Integer> combinedValues = new ArrayList<>(leftTuple.getValues());
        combinedValues.addAll(rightTuple.getValues());
        return new Tuple(combinedValues);
    }

    /**
     * Resets the state of the join operator to start producing tuples from the beginning.
     */
    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
        leftTuple = leftChild.getNextTuple();
        rightTuple = rightChild.getNextTuple();
    }
}
