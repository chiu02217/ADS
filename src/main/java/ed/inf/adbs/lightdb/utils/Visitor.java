package ed.inf.adbs.lightdb.utils;
import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.List;
import java.util.Map;

/**
 *
 *  class to handle predicate
  */
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
    // for mapping the indexs
    private Map<Integer, Integer> mapping = null;

    public Map<Integer, Integer> getMapping() {
        return mapping;
    }

    public void setMapping(Map<Integer, Integer> mapping) {
        this.mapping = mapping;
    }

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

    /**
     * whether predicate conform to a specific expression
     * @param tuple
     * @param tableName
     * @param expr : JSQLParser Expression tree
     * @return
     */
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

    /**
     * get the value of the specific column of the current tuple
     * @param column Column
     */
    @Override
    public void visit(Column column) {
//        String tableName = column.getTable().getName();
//        if (tableName == null) {
//            throw new RuntimeException("no table");
//        }
//        String columnName = column.getColumnName();
//        // get index
//        int index = ColumnHelper.getColumnIndexAfterJoin(column, this.joinTables);
//        currentValue = currentTuple.getKeyValue(index);
        // 計算原始全域索引
        int originalIndex = ColumnHelper.getColumnIndexAfterJoin(column, joinTables);
        // 如果有 projection push-down，透過 ColumnMapping 轉換成新位置
        int tupleIndex = (mapping != null)
                ? ColumnHelper.getValueAfterRemap(mapping, originalIndex)
                : originalIndex;
        currentValue = currentTuple.getKeyValue(tupleIndex);
    }

    /**
     * >
     * @param greaterThan
     */
    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        greaterThan.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf > rightLeaf;
    }

    /**
     * <
     * @param minorThan
     */
    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        minorThan.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf < rightLeaf;
        //System.out.println("Debug: " + leftLeaf + " < " + rightLeaf + " = " + currentResult);
    }

    /**
     * ==
     * @param equalsTo
     */
    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        equalsTo.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf == rightLeaf;
    }

    /**
     * !=
     * @param notEqualsTo
     */
    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        notEqualsTo.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = leftLeaf != rightLeaf;
    }

    /**
     * >=
     * @param greaterThanEquals
     */
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        greaterThanEquals.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = (leftLeaf >= rightLeaf);
    }

    /**
     * <=
     * @param minorThanEquals
     */
    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        int leftLeaf = currentValue;
        minorThanEquals.getRightExpression().accept(this);
        int rightLeaf = currentValue;
        currentResult = (leftLeaf <= rightLeaf);
    }

    /**
     * AND
     * @param andExpression
     */
    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean leftLeaf = currentResult;
        andExpression.getRightExpression().accept(this);
        boolean rightLeaf = currentResult;
        currentResult = leftLeaf && rightLeaf;
    }

    /**
     * OR
     * @param orExpression
     */
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

    /**
     * * : multuply
     * @param multiplication
     */
    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        int left = currentValue;
        multiplication.getRightExpression().accept(this);
        currentValue = left * currentValue; // 計算乘法結果
    }

    /**
     * +
     * @param addition
     */
    @Override
    public void visit(Addition addition) {
        addition.getLeftExpression().accept(this);
        int left = currentValue;
        addition.getRightExpression().accept(this);
        currentValue = left + currentValue;
    }
}
