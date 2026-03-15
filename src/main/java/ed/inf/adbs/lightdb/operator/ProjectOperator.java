package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
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
    /**
     * Collects every global column index referenced after the join tree,
     * i.e. by SELECT, GROUP BY, ORDER BY, and aggregate expressions.
     *
     * @param plainSelect the parsed SQL statement
     * @param tables      all table names in FROM order
     * @return sorted list of needed global indices
     */
    public static List<Integer> collectNeededColumns(PlainSelect plainSelect, List<String> tables) {
        Set<Integer> needed = new LinkedHashSet<>();

        // SELECT clause
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
     * Recursively walks an expression and adds the global index of every Column
     * reference to the needed set.
     *
     * @param expr   expression to walk
     * @param tables all table names in FROM order
     * @param needed accumulates discovered global indices
     */
    private static void collectColumnsFromExpr(Expression expr, List<String> tables,
                                               Set<Integer> needed) {
        if (expr == null) return;
        if (expr instanceof Column) {
            needed.add(getColumnIndexAfterJoin(expr, tables));
        } else if (expr instanceof Multiplication) {
            Multiplication mul = (Multiplication) expr;
            collectColumnsFromExpr(mul.getLeftExpression(), tables, needed);
            collectColumnsFromExpr(mul.getRightExpression(), tables, needed);
        } else if (expr instanceof Addition) {
            Addition add = (Addition) expr;
            collectColumnsFromExpr(add.getLeftExpression(), tables, needed);
            collectColumnsFromExpr(add.getRightExpression(), tables, needed);
        }
    }


    @Override
    public void reset() {
        inputSource.reset();
    }
}
