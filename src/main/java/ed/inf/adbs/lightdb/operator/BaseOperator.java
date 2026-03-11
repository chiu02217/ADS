package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
/**
 * lowest level tof operator
 */
public abstract class BaseOperator {

    /**
     * Retrieves the next tuple from the iterator.
     * @return A Tuple object representing the row of data, or NULL if EOF reached.
     */
    public abstract Tuple getNextTuple();

    /**
     * Resets the iterator to the start.
     */
    public abstract void reset();
}