package ed.inf.adbs.lightdb;

import java.io.PrintStream;


/**
 * The {@code Operator} abstract class serves as the foundation for implementing various database operators.
 * It defines the essential operations that all concrete operators must implement, such as fetching the next tuple
 * and resetting the operator's state. Additionally, it provides a utility method to output all tuples
 * produced by the operator.
 */
public abstract class Operator {

    /**
     * Retrieves the next tuple from the operator. This method is called repeatedly to iterate through
     * all tuples produced by the operator until it returns {@code null}, indicating that there are no more tuples.
     *
     * @return The next {@link Tuple} produced by the operator, or {@code null} if there are no more tuples.
     */
    public abstract Tuple getNextTuple();

    /**
     * Resets the operator's state to its initial condition. This method allows the operator to be reused,
     * starting the tuple iteration from the beginning.
     */
    public abstract void reset();



    public void dump(PrintStream out) {
        Tuple tuple;
        while ((tuple = getNextTuple()) != null) {
            out.println(tuple);
        }
    }
}
