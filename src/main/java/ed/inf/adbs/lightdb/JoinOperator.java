//package ed.inf.adbs.lightdb;
//
//import net.sf.jsqlparser.JSQLParserException;
//import net.sf.jsqlparser.expression.Expression;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class JoinOperator extends Operator {
//    private Operator leftChild;
//    private Operator rightChild;
//    private Expression joinCondition;
//    private Tuple currentLeftTuple;
//    private Schema combinedSchema; // New member for combined schema
//
//    private Tuple leftTuple;
//    private Tuple rightTuple;
//
//
//
//    public JoinOperator(Operator leftChild, Operator rightChild, Expression joinCondition, Schema combinedSchema) {
//        this.leftChild = leftChild;
//        this.rightChild = rightChild;
//        this.joinCondition = joinCondition;
//        this.currentLeftTuple = null;
//        this.combinedSchema = combinedSchema;
//
//        leftTuple = leftChild.getNextTuple();
//        rightTuple = rightChild.getNextTuple();
//
//    }
//
//    @Override
//    public Tuple getNextTuple() {
//        try {
//            Tuple t = null;
//            while (leftTuple != null) { //while we still have tuples from the outer table to explore
//                JoinEvalExpression jee = new JoinEvalExpression(leftTuple, rightTuple, joinCondition, combinedSchema);
//                //if there is no expression (cartesian product) or the tuples fulfill the join condition
//                if (joinCondition == null || Boolean.parseBoolean(jee.evaluate().toString()))
//
//                t = combineTuples(leftTuple, rightTuple); //create a new tuple that has all the attributes of both tuples
//                //update tuples
//                if (rightTuple != null) //get next tuple for the inner table
//                    rightTuple = rightChild.getNextTuple();
//
//                if (rightTuple == null) { //if it was the last one
//                    rightChild.reset(); //reset the inner operator (to start the inner table scan again)
//                    rightTuple = rightChild.getNextTuple(); //get the first tuple of the inner table
//                    leftTuple = leftChild.getNextTuple(); //get the next outer table's tuple
//                }
//                if (t != null) //after updating the tuples, we check if we got a tuple that fulfills the join condition
//                    return t; //if so we return it
//            }
//        } catch (JSQLParserException ex) {
//            ex.printStackTrace();
//        }
//        return null; //if no tuple fulfills the condition we return nulll
//    }
//
//
//    private Tuple combineTuples(Tuple leftTuple, Tuple rightTuple) {
//        List<Integer> leftValues = leftTuple.getValues();
//        List<Integer> rightValues = rightTuple.getValues();
//
//        // Create a new list to hold combined values
//        List<Integer> combinedValues = new ArrayList<>(leftValues);
//        combinedValues.addAll(rightValues); // Add all values from the right tuple
//
//        return new Tuple(combinedValues); // Return a new tuple with combined values
//    }
//
////    private boolean evaluateJoinCondition(Tuple leftTuple, Tuple rightTuple) {
////        JoinEvalExpression evalExpression = new JoinEvalExpression(combinedSchema);
////
////        return evalExpression.evaluate(joinCondition, leftTuple, rightTuple);
////    }
//
//    @Override
//    public void reset() {
//        leftChild.reset();
//        rightChild.reset();
//        currentLeftTuple = null;
//    }
//}


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
