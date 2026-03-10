package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;

import java.io.BufferedReader;
import java.io.IOException;

public abstract class Operator extends BaseOperator {
    protected BufferedReader reader;
    protected String tablePath;



    @Override
    public Tuple getNextTuple() {
        try {
            return _getNextTuple();
        } catch (IOException e) {
            System.err.println("get next tuple error:" + this.getClass().getSimpleName());
            throw new RuntimeException(e);
        }

    }

    protected abstract Tuple _getNextTuple() throws IOException;
    @Override
    public abstract void reset();

}