package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  class to deal with scan the table
 *  if the comments are not clear enough, please refer to README.md
 */
public class ScanOperator extends Operator{
    // used for push-downo, only needed columns are returned
    private List<Integer> projectedCols;

    public ScanOperator(String tablePath) {
        this.tablePath = tablePath;
        this.projectedCols = null;
        this.reset();
    }

    /**
     * push-down constructor, only returns the specified column indexs.
     * @param tablePath
     * @param projectedCols null = full scan(all columns)
     */
    public ScanOperator(String tablePath, List<Integer> projectedCols) {
        this.tablePath = tablePath;
        this.projectedCols = projectedCols;
        this.reset();
    }

    @Override
    protected Tuple _getNextTuple() throws IOException {
        String line = reader.readLine();
        if (line == null) return null;
        Tuple full = new Tuple(line);
        if (projectedCols == null) return full;
        // for push-down, only keep the needed columns
        List<Integer> projected = new ArrayList<>();
        for (int index : projectedCols) {
            projected.add(full.getKeyValue(index));
        }
        return new Tuple(projected);
    }
    @Override
    public void reset() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            // read table
            this.reader = new BufferedReader(new FileReader(tablePath));
        }
        catch (IOException e) {
            System.err.println("scan operator reset error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
