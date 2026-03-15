package ed.inf.adbs.lightdb.utils;
import ed.inf.adbs.lightdb.schema.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * class that deal with the computation of columns
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
     * return the culumn indexs used to groupby
     * @param groupbyColList
     * @param tables
     * @return
     */
    public static List<Integer> getGroupByColIndexs(List<Object> groupbyColList, List<String> tables) {
        List<Integer> indexs = new ArrayList<>();
        for (Object col : groupbyColList) {
            Expression expr = (Expression) col;
            indexs.add(ColumnHelper.getColumnIndexAfterJoin(expr, tables));
        }
        return indexs;
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
     * projection push-down removes unneeded columns.
     *
     *  Ex:
     *  Original joined tuple: [A=0, B=1, C=2, D=3, E=4]
     *  needed columns indexs = [0, 4]  ( A and E)
     *  Mapping([0, 4]) → [0→0, 4→1]
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
     * Translates an original global column index to its new compact position(index)
     * using a mapping produced by columnMapping().
     * ex: mapping = { 0→0, 4→1 }, then getKeyValue(0) getKeyValue(1)
     * @param mapping       map from original global index to compact position
     * @param originalIndex the column's index in the full joined tuple
     * @return
     * @throws RuntimeException
     */
    public static int getValueAfterRemap(Map<Integer, Integer> mapping, int originalIndex) {
        Integer pos = mapping.get(originalIndex);
        if (pos == null) {
            throw new RuntimeException(
                    "Column index " + originalIndex + " not found");
        }
        return pos;
    }
    /**
     * Collects every global column index referenced after the join tree,
     * i.e. by SELECT, GROUP BY, ORDER BY, and aggregate expressions.
     * ex:
     *  Original joined tuple: [A=0, B=1, C=2, D=3, E=4]
     *  needed columns indexs = [0, 4]  ( A and E)
     * @param plainSelect  SQL
     * @param tables      all table names
     * @return sorted list of needed global indices
     */
    public static List<Integer> collectNeededColumnsAfterJoin(PlainSelect plainSelect, List<String> tables) {
        Set<Integer> needed = new HashSet<>();

        // SELECT
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof Function) {
                Function f = (Function) expr;
                if (f.getParameters() != null) {
                    for (Object param : f.getParameters().getExpressions()) {
                        collectColumnsFromExpr((Expression) param, tables, needed);
                    }
                }
            } else {
                collectColumnsFromExpr(expr, tables, needed);
            }
        }

        // GROUP BY
        if (plainSelect.getGroupBy() != null) {
            for (Object obj : plainSelect.getGroupBy().getGroupByExpressionList()) {
                collectColumnsFromExpr((Expression) obj, tables, needed);
            }
        }

        // ORDER BY
        if (plainSelect.getOrderByElements() != null) {
            for (OrderByElement elem : plainSelect.getOrderByElements()) {
                collectColumnsFromExpr(elem.getExpression(), tables, needed);
            }
        }

        List<Integer> result = new ArrayList<>(needed);
        Collections.sort(result);
        return result;
    }

    /**
     * Recursively walks an expression and collects the distinct table names
     * of all Column references found.
     * Used by WhereDecomposer
     * @param expr   expression to walk
     * @param result accumulates distinct table names found
     */
    public static void collectTableNames(Expression expr, List<String> result) {
        walkColumns(expr, col -> {
            if (col.getTable() != null && col.getTable().getName() != null) {
                String tableName = col.getTable().getName();
                if (!result.contains(tableName)) result.add(tableName);
            }
        });
    }

    /**
     * address a single expression
     * Used by collectNeededColumns
     * @param expr
     * @param tables table names FROM
     * @param needed
     */
    private static void collectColumnsFromExpr(Expression expr, List<String> tables,
                                               Set<Integer> needed) {
        walkColumns(expr, col -> needed.add(getColumnIndexAfterJoin(col, tables)));
    }
    /**
     * Recursively walks an expression and calls callback for every Column found.
     * Used by collectColumnsFromExpr
     * @param expr
     * @param callback action to perform on each Column node
     */
    private static void walkColumns(Expression expr, Consumer<Column> callback) {
        if (expr == null) return;
        if (expr instanceof Column) {
            callback.accept((Column) expr);
        } else if (expr instanceof Multiplication) {
            walkColumns(((Multiplication) expr).getLeftExpression(), callback);
            walkColumns(((Multiplication) expr).getRightExpression(), callback);
        } else if (expr instanceof Addition) {
            walkColumns(((Addition) expr).getLeftExpression(), callback);
            walkColumns(((Addition) expr).getRightExpression(), callback);
        } else if (expr instanceof ComparisonOperator) {
            walkColumns(((ComparisonOperator) expr).getLeftExpression(), callback);
            walkColumns(((ComparisonOperator) expr).getRightExpression(), callback);
        }
    }
}
