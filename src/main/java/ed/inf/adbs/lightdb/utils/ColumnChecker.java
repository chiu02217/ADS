package ed.inf.adbs.lightdb.utils;
import ed.inf.adbs.lightdb.schema.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;

public class ColumnChecker {
    public static int checkAndGetIndex(Expression expr, String tableName, String error) throws IOException {
        if (!(expr instanceof Column)) {
            throw new IOException(error);
        }
        Column col = (Column) expr;
        String colName = col.getColumnName();
        int index = Schema.getInstance().getColumnIndex(tableName, colName);
        if (index == -1) {
            throw new IOException(error);
        }
        return index;
    }
}
