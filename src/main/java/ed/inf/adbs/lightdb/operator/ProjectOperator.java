package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static ed.inf.adbs.lightdb.utils.ColumnChecker.getColumnIndexAfterJoin;

/**
 * class to handle project (SELECT)
 */
public class ProjectOperator extends Operator {
    private Operator inputSource;
    // all fields after the SELECT and before the FROM
    private List<SelectItem<?>> selectItems;
    private String tableName;
    // tables which wait for table
    private List<String> joinTables;

    // need this to calculate the offset of the new index of columns
    private int groupByCount;

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

    /**
     * getNextTuple but considering groupby and join conditions
     * @return
     */
    @Override
    protected Tuple _getNextTuple() {
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
            int funcCount = 0;
            for (SelectItem<?> item : selectItems) {
                Expression expr = item.getExpression();
                // like MAX() SUM()
                if (expr instanceof Function) {
                    // Aggregate result sits after all group-by columns
                    selectedCols.add(nextTuple.getKeyValue(groupByCount + funcCount));
                    funcCount++;
                } else {
                    // Regular column: find its position inside the group-by output block
                    int pos = getColumnIndexAfterGroupBy(expr);
                    selectedCols.add(nextTuple.getKeyValue(pos));
                }
            }
        }
        // no GROUPBY but aggregation tasks(SUM, MAX)
        // tuple's format:
        // [groupbyCol1, groupbyCols2, aggregationTask1, aggregationTask2, regularCol1, ...]
        else {
            int funcCount = 0;
            for (SelectItem<?> item : selectItems) {
                Expression expr = item.getExpression();
                if (expr instanceof Function) {
                    selectedCols.add(nextTuple.getKeyValue(funcCount));
                    funcCount++;
                } else {
                    int index = getColumnIndexAfterJoin(expr, joinTables);
                    selectedCols.add(nextTuple.getKeyValue(index));
                }
            }
        }
        return new Tuple(selectedCols);
    }

    /**
     * find the index of specific column after groupby
     * @param expr
     * @return
     */
    private int getColumnIndexAfterGroupBy(Expression expr) {
        // this function can handle both join and no join conditions
        int index = getColumnIndexAfterJoin(expr, joinTables);
        // index of specific column after groupby
        int pos = 0;
        // scan SELECT columns
        for (SelectItem<?> item : selectItems) {
            Expression e = item.getExpression();
            // skip aggregation tasks, just look into regular column
            if (!(e instanceof Function)) {
                // calculate the index of current SELECT column, check if it is the same as the target column.
                if (getColumnIndexAfterJoin(e, joinTables) == index) {
                    // true and return
                    return pos;
                }
                // next
                pos++;
            }
        }
        throw new RuntimeException("Column not found in GROUPBY output: " + expr);
    }
    @Override
    public void reset() {
        inputSource.reset();
    }
}
