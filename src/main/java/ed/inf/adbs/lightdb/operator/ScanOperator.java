package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ScanOperator extends BaseOperator{
    public ScanOperator(String tableName, String tablePath) {
        this.tablePath = tablePath;
        this.reset();
    }


    /// The main getNextTuple()
    @Override
    protected Tuple _getNextTuple() throws IOException {
        String tuple = reader.readLine();
        //return new Tuple();
        return (tuple != null) ? new Tuple(tuple) : null;
    }
    @Override
    public void reset() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            // read table
            this.reader = new BufferedReader(new FileReader(tablePath));
        } catch (IOException e) {
            System.err.println("ScanOperator reset error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
