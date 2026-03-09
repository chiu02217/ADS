package ed.inf.adbs.lightdb;

import java.util.List;

/**
 * The Tuple class represents a row of data.
 *
 * You will need to modify this class, obviously :).
 */
import java.util.ArrayList;
import java.util.List;

public class Tuple {
    private List<Integer> cols;

    // constructor for select
    public Tuple(String tuple) {
        this.cols = new ArrayList<>();
        if (tuple != null) {
            // ex: 1, 2, 3 => [1,2,3]
            String[] attrs = tuple.trim().split(",");
            for (String attr : attrs) {
                if (attr !=null && !attr.equals(" ")) {
                    this.cols.add(Integer.parseInt(attr.trim()));
                }
                else {
                    this.cols.add(null);
                }
            }
        }
    }
    // constructor for project
    public Tuple(List<Integer> cols) {
        this.cols = new ArrayList<>(cols);
    }
    ///
    ///
    ///
    @Override
    public String toString() {
        if (cols == null || cols.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            sb.append(cols.get(i));
            if (i < cols.size() - 1) sb.append(",");
        }
        return sb.toString();
    }

    public List<Integer> getCols() {
        return cols;
    }

    public int getKeyValue(int index) {
        if (index >= 0 && index < cols.size()) {
            return cols.get(index);
        }
        throw new IndexOutOfBoundsException("Tuple index out of range: " + index);
    }
}