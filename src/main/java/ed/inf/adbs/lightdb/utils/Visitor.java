package ed.inf.adbs.lightdb.utils;
import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.schema.Schema;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/// class to handle predicate
public class Visitor extends ExpressionVisitorAdapter{
    private Tuple currentTuple;
    // store most recent value of the leaf
    private int currentValue;
    // store most recent result of boolean operation
    private boolean currentResult;
    // current tableName
    private String tableName;
    // join table
    private List<String> joinTables;


    public Tuple getCurrentTuple() {
        return currentTuple;
    }

    public void setCurrentTuple(Tuple currentTuple) {
        this.currentTuple = currentTuple;
    }

    public List<String> getJoinTables() {
        return joinTables;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(int currentValue) {
        this.currentValue = currentValue;
    }

    public boolean isCurrentResult() {
        return currentResult;
    }

    public void setCurrentResult(boolean currentResult) {
        this.currentResult = currentResult;
    }

    public boolean evaluate(Tuple tuple, String tableName, Expression expr) {
        this.currentTuple = tuple;
        this.tableName = tableName;
        // expression tree
        expr.accept(this);
        return currentResult;
    }
    public void setJoinTables(List<String> joinTables) {
        this.joinTables = joinTables;
    }
    @Override
    public void visit(LongValue value) {
        currentValue = (int) value.getValue();
    }

    //  WHERE A > 5
    @Override
    public void visit(Column column) {
        String tableName = column.getTable().getName();
        if (tableName == null) {
            throw new RuntimeException("no table");
        }
        String columnName = column.getColumnName();
        // get index
        int index = getJoinColumnNum(tableName, columnName);
        if (index == -1) {
            throw new RuntimeException("Column Not Found in " + tableName + "." + columnName);
        }
        currentValue = currentTuple.getKeyValue(index);
    }

    // >
    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        greaterThan.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf > rightLeaf;
    }
    // <
    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        minorThan.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf < rightLeaf;
        //System.out.println("Debug: " + leftLeaf + " < " + rightLeaf + " = " + currentResult);
    }

    // ==
    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        equalsTo.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf == rightLeaf;
    }
    // !=
    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        notEqualsTo.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf != rightLeaf;
    }
    // >=
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        greaterThanEquals.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = (leftLeaf >= rightLeaf);
    }
    // <=
    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        minorThanEquals.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = (leftLeaf <= rightLeaf);
    }

    //  AND &&
    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftLeaf = currentResult;
        andExpression.getRightExpression().accept(this);
        boolean rightLeaf = currentResult;
        currentResult = leftLeaf && rightLeaf;
    }
    // OR ||
    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        boolean leftCondition = currentResult;
        if (leftCondition) {
            currentResult = true;
            return;
        }
        orExpression.getRightExpression().accept(this);
    }
    // *
    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        int left = currentValue;
        multiplication.getRightExpression().accept(this);
        currentValue = left * currentValue; // 計算乘法結果
    }

    @Override
    public void visit(Addition addition) {
        addition.getLeftExpression().accept(this);
        int left = currentValue;
        addition.getRightExpression().accept(this);
        currentValue = left + currentValue;
    }
    // only for join
    private int getJoinColumnNum(String tableName, String columnName) {
        // 1. 取得所有參與 Join 的表名列表（這需要從執行計畫傳進來）
        List<String> tables = this.joinTables;
        int offset = 0;

        for (String t : tables) {
            if (t.equals(tableName)) {
                // 找到目標表，加上它在該表內的原始索引
                return offset + Schema.getInstance().getColumnIndex(t, columnName);
            }
            // 如果不是這張表，就加上這張表的總欄位數，繼續往後跳
            offset += Schema.getInstance().getNumberOfTableCol(t).size();
        }
        throw new RuntimeException("Column not found in joined tables: " + tableName + "." + columnName);
    }
}
