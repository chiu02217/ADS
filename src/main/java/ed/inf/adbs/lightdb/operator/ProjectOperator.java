package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static ed.inf.adbs.lightdb.utils.ColumnChecker.getColumnIndexAfterJoin;

public class ProjectOperator extends Operator {
    private Operator inputSource;
    // all fields after the SELECT and before the FROM
    private List<SelectItem<?>> selectItems;
    private String tableName; // 用於查找欄位索引
    // fix
    private List<String> joinTables;

    // === FIX: number of GROUP BY columns in the AggregateOperator output tuple.
    //          0 means no aggregation — use original table schema indices.
    //          >0 means after GROUP BY: tuple layout is [groupByCols..., aggResults...] ===
    private int groupByCount;

    public ProjectOperator(Operator inputSource, String tableName, List<SelectItem<?>> selectItems) {
        this.inputSource = inputSource;
        this.tableName = tableName;
        this.selectItems = selectItems;
        this.joinTables = Collections.singletonList(tableName);
        this.groupByCount = 0;
    }
    public ProjectOperator(Operator inputSource, List<String> joinTables,
                           List<SelectItem<?>> selectItems, int groupByCount) {
        this.inputSource = inputSource;
        this.joinTables = joinTables;
        this.tableName = joinTables.isEmpty() ? null : joinTables.get(0);
        this.selectItems = selectItems;
        this.groupByCount = groupByCount;
    }

    public Operator getInputSource() {
        return inputSource;
    }

    public void setInputSource(Operator inputSource) {
        this.inputSource = inputSource;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<SelectItem<?>> getSelectItems() {
        return selectItems;
    }

    public void setSelectItems(List<SelectItem<?>> selectItems) {
        this.selectItems = selectItems;
    }

    @Override
    protected Tuple _getNextTuple() throws IOException {
        Tuple nextTuple = inputSource.getNextTuple();
        if (nextTuple == null) {
            return null;
        }

        // Select * condition
        if (selectItems.get(0).toString().equals("*")) {
            return nextTuple;
        }
        // the others condition
        List<Integer> selectedCols = new ArrayList<>();
        if (groupByCount>0) {
            int count = 0;
            for (SelectItem<?> item : selectItems) {
                Expression expr = item.getExpression();
                if (expr instanceof Function) {
                    // Aggregate result sits after all group-by columns
                    selectedCols.add(nextTuple.getKeyValue(groupByCount + count));
                    count++;
                } else {
                    // Regular column: find its position inside the group-by output block
                    int pos = findGroupByPosition(expr);
                    selectedCols.add(nextTuple.getKeyValue(pos));
                }
            }
        }
        // 修復：無 GROUP BY 但有聚合函數時（e.g. SELECT SUM(1), SUM(A) FROM ...）
        // AggregateOperator 輸出 Tuple 格式：[aggResult_0, aggResult_1, ...]
        // Function 用 aggFuncCounter 當索引；Column 用 calculateGlobalIndex
        else {
            int aggFuncCounter = 0;
            for (SelectItem<?> item : selectItems) {
                Expression expr = item.getExpression();
                if (expr instanceof Function) {
                    selectedCols.add(nextTuple.getKeyValue(aggFuncCounter));
                    aggFuncCounter++;
                } else {
                    int index = getColumnIndexAfterJoin(expr, joinTables);
                    selectedCols.add(nextTuple.getKeyValue(index));
                }
            }
        }
        return new Tuple(selectedCols);
    }
    // -----------------------------------------------------------------------
    // Find which position (0-based) this column occupies in the GROUP BY list
    // that AggregateOperator preserves as the first groupByCount slots.
    //
    // AggregateOperator writes group-by columns in the order it first sees
    // each distinct group key — which matches the global join index order.
    // So we use the global index and scan the GROUP BY select items to find
    // the matching slot.
    // -----------------------------------------------------------------------
    private int findGroupByPosition(Expression expr) {
        int index = getColumnIndexAfterJoin(expr, joinTables);
        int pos = 0;
        for (SelectItem<?> item : selectItems) {
            Expression e = item.getExpression();
            if (!(e instanceof Function)) {
                if (getColumnIndexAfterJoin(e, joinTables) == index) {
                    return pos;
                }
                pos++;
            }
        }
        throw new RuntimeException("Column not found in GROUP BY output: " + expr);
    }
    // -----------------------------------------------------------------------
    // Calculate the column's 0-based index across all joined tables,
    // matching the logic in Visitor.getJoinColumnNum() and Parser.
    // -----------------------------------------------------------------------
//    private int calculateGlobalIndex(Expression expr) {
//        if (expr instanceof Column) {
//            Column col = (Column) expr;
//            String tName = (col.getTable() != null && col.getTable().getName() != null)
//                    ? col.getTable().getName()
//                    : joinTables.get(0);
//            String colName = col.getColumnName();
//            int offset = 0;
//            for (String t : joinTables) {
//                if (t.equals(tName)) {
//                    return offset + Schema.getInstance().getColumnIndex(t, colName);
//                }
//                offset += Schema.getInstance().getNumberOfTableCol(t).size();
//            }
//            throw new RuntimeException("Column not found in joined tables: " + tName + "." + colName);
//        }
//        throw new RuntimeException("ProjectOperator: unsupported expression type: " + expr);
//    }
    @Override
    public void reset() {
        inputSource.reset();
    }
}
