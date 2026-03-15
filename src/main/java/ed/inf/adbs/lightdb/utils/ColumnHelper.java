package ed.inf.adbs.lightdb.utils;
import ed.inf.adbs.lightdb.schema.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * class that address computation of columns
 */
public class ColumnHelper {
    public static int checkAndGetIndex(Expression expr, String tableName, String error) throws IOException {
        if (!(expr instanceof Column)) {
            throw new IOException(error);
        }
        String colName = ((Column) expr).getColumnName();
        int index = Schema.getInstance().getColumnIndex(tableName, colName);
        if (index == -1) {
            throw new IOException(error);
        }
        return index;
    }
    /**
     * Calculates the absolute index position of the specific field in the tuple after join.
     * ** can also deal with no "JOIN" condition
     * @param expr
     * @param tables: Joined tables
     * @return int
     */
    public static int getColumnIndexAfterJoin(Expression expr, List<String> tables) {
        if (!(expr instanceof Column)) {
            throw new RuntimeException("Column not found in joined tables: " + expr);
        }
        Column col = (Column) expr;
//        String tableName = (col.getTable() != null && col.getTable().getName() != null)
//                ? col.getTable().getName()
//                : tables.get(0);
        String tableName = col.getTable().getName();
        String colName = ((Column) expr).getColumnName();
        int offset = 0;
        for (String table : tables) {
            if (table.equals(tableName)) {
                // The original index of the column in the table
                return offset + Schema.getInstance().getColumnIndex(table, colName);
            }
            offset += Schema.getInstance().getNumberOfTableCol(table).size();
        }
        return offset;
    }

    /**
     * Maps original global column indices to new compact positions after
     *      * projection push-down removes unneeded columns.
     *      *
     *      * Example:
     *      *   Original joined tuple: [A=0, B=1, C=2, D=3, E=4]
     *      *   indexs = [0, 4]  (only A and E are needed downstream)
     *      *   buildMapping([0, 4]) → {0→0, 4→1}
     *      *   Projected tuple: [A_val, E_val]
     * @param indexs
     * @return
     */
    public static Map<Integer, Integer> columnMapping(List<Integer> indexs) {
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int i = 0; i < indexs.size(); i++) {
            mapping.put(indexs.get(i), i);
        }
        return mapping;
    }
    /**
     * Translates an original global column index to its new compact position
     * using a mapping produced by buildMapping().
     *
     * @param mapping             map from original global index to compact position
     * @param originalIndex the column's index in the full joined tuple
     * @return the column's index in the projected tuple
     * @throws RuntimeException if the index was not retained in the mapping
     */
    public static int remap(Map<Integer, Integer> mapping, int originalIndex) {
        Integer pos = mapping.get(originalIndex);
        if (pos == null) {
            throw new RuntimeException(
                    "Column index " + originalIndex + " not found in projected layout");
        }
        return pos;
    }
}
