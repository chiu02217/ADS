package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.io.IOException;
import java.util.*;

import static ed.inf.adbs.lightdb.utils.ColumnHelper.getColumnIndexAfterJoin;

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
    private List<Integer> indexs = Collections.emptyList();


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
    /**
     * Intermediate projection constructor (projection push-down).
     * Keeps only the columns at indexs, used between the join tree
     * and GROUP BY / ORDER BY to reduce tuple width.
     *
     * @param inputSource  upstream operator
     * @param indexs  sorted list of original global indices to retain
     */
    public ProjectOperator(Operator inputSource, List<Integer> indexs) {
        this.inputSource = inputSource;
        this.indexs = indexs;
        this.selectItems = null;   // signals intermediate mode
        this.joinTables = Collections.emptyList();
        this.groupByCount = 0;
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
        // Intermediate projection mode: just keep the cols we need for final answer
        if (selectItems == null) {
            List<Integer> projected = new ArrayList<>();
            for (int index : indexs) {
                projected.add(nextTuple.getKeyValue(index));
            }
            return new Tuple(projected);
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
