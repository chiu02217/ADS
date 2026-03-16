package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScanOperator extends Operator{
    // scan push-down: if set, only these local (within-table) column indices are returned
    private List<Integer> projectedLocalCols;

    public ScanOperator(String tablePath) {
        this.tablePath = tablePath;
        this.projectedLocalCols = null;
        this.reset();
    }

    /**
     * Scan push-down constructor: only returns the specified local column indices.
     * @param tablePath        path to the CSV file
     * @param projectedLocalCols local (0-based within-table) indices to retain; null = full scan
     */
    public ScanOperator(String tablePath, List<Integer> projectedLocalCols) {
        this.tablePath = tablePath;
        this.projectedLocalCols = projectedLocalCols;
        this.reset();
    }

    @Override
    protected Tuple _getNextTuple() throws IOException {
        String line = reader.readLine();
        if (line == null) return null;
        Tuple full = new Tuple(line);
        if (projectedLocalCols == null) return full;
        // scan push-down: only keep the needed local columns
        List<Integer> projected = new ArrayList<>();
        for (int idx : projectedLocalCols) {
            projected.add(full.getKeyValue(idx));
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
