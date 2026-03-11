package ed.inf.adbs.lightdb.utils;
import ed.inf.adbs.lightdb.schema.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.util.List;

/**
 * class that address computation of columns
 */
public class ColumnChecker {
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
}
