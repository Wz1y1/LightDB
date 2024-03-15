package ed.inf.adbs.lightdb;

import java.io.PrintStream;

public abstract class Operator {
    public abstract Tuple getNextTuple();
    public abstract void reset();

    public void dump(PrintStream out) {
        Tuple tuple;
        while ((tuple = getNextTuple()) != null) {
            out.println(tuple);
        }
    }
}
