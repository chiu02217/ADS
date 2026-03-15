package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.utils.Visitor;
import net.sf.jsqlparser.expression.Expression;
import java.util.*;

/**
 * class to handle aggregation tasks (GROUPBY, SUM, ...)
 */
public class AggregateOperator extends Operator{
    private List<String> aggregationTasks;
    // aggregate by which column(index)
    private Operator inputSource;
    // stored cols' indexs which we use to group by
    private List<Integer> groupByIndexs;
    private Iterator<Tuple> resultList;
    private List<Expression> aggregationExpr;
    private Visitor visitor;
    // for projection push down (global original index -> index after projection)
    private Map<Integer, Integer> mapping = null;

    public List<String> getAggregationTasks() {
        return aggregationTasks;
    }

    public void setAggregationTasks(List<String> aggregationTasks) {
        this.aggregationTasks = aggregationTasks;
    }

    public Operator getInputSource() {
        return inputSource;
    }

    public void setInputSource(Operator inputSource) {
        this.inputSource = inputSource;
    }

    public List<Integer> getGroupByIndexs() {
        return groupByIndexs;
    }

    public void setGroupByIndexs(List<Integer> groupByIndexs) {
        this.groupByIndexs = groupByIndexs;
    }

    public Iterator<Tuple> getResultList() {
        return resultList;
    }

    public void setResultList(Iterator<Tuple> resultList) {
        this.resultList = resultList;
    }

    public List<Expression> getAggregationExpr() {
        return aggregationExpr;
    }

    public void setAggregationExpr(List<Expression> aggregationExpr) {
        this.aggregationExpr = aggregationExpr;
    }

    public Visitor getVisitor() {
        return visitor;
    }

    public void setVisitor(Visitor visitor) {
        this.visitor = visitor;
    }

    public Map<Integer, Integer> getMapping() {
        return mapping;
    }

    public void setMapping(Map<Integer, Integer> mapping) {
        this.mapping = mapping;
        this.visitor.setMapping(mapping);
    }

    public AggregateOperator(Operator input, List<Integer> groupByIndexss, List<String> aggregationTasks, List<Expression> aggregationExpr, List<String> tables) {
        this.inputSource = input;
        this.groupByIndexs = groupByIndexss;
        this.aggregationTasks = aggregationTasks;
        this.aggregationExpr = aggregationExpr;
        this.visitor = new Visitor();
        this.visitor.setJoinTables(tables);
    }

    /**
     * get the value of expression tree for aggregation tasks
     * @param expr
     * @param tuple
     * @return
     */
    private int evaluate(Expression expr, Tuple tuple) {
        visitor.setCurrentTuple(tuple);
        // visitor goes through the expr
        expr.accept(visitor);
        return visitor.getCurrentValue();
    }
    @Override
    protected Tuple _getNextTuple(){
        if (resultList == null) {
            resultList = aggregate();
        }

        if (resultList.hasNext()) {
            return resultList.next();
        }
        return null;
    }

    /**
     * func to handle aggregation tasks
     * @return
     */
    private Iterator<Tuple> aggregate() {
        Tuple tuple;
        // groups <key, value>
        Map<List<Integer>, List<Integer>> groups = new HashMap<>();

        while ((tuple = inputSource.getNextTuple()) != null) {
            // grpupByKeys
            List<Integer> groupByKeys = new ArrayList<>();
            for (int index : groupByIndexs) {
                groupByKeys.add(tuple.getKeyValue(index));
            }

            List<Integer> groupByValues = groups.get(groupByKeys);
            // if not found, means that this key is newly added. Initial the values!
            if (groupByValues == null) {
                List<Integer> vals = new ArrayList<>();
                for (int i = 0; i < aggregationTasks.size(); i++) {
                    String task = aggregationTasks.get(i).toUpperCase();
                    // compute. ex: H*H
                    int val = evaluate(aggregationExpr.get(i), tuple);
                    vals.add(task.equals("COUNT") ? 1 : val);
                }
                groups.put(groupByKeys, vals);

            }
            // if the initial values exist, then the current tuple value does the aggregation task with the previous tuples
            else {
                for (int i = 0; i < aggregationTasks.size(); i++) {
                    String task = aggregationTasks.get(i).toUpperCase();
                    int newVal = evaluate(aggregationExpr.get(i), tuple);
                    int currentVal = groupByValues.get(i);
                    // main
                    switch (task) {
                        case "SUM":
                            groupByValues.set(i, currentVal + newVal);
                            break;
                        case "MIN":
                            groupByValues.set(i, Math.min(currentVal, newVal));
                            break;
                        case "MAX":
                            groupByValues.set(i, Math.max(currentVal, newVal));
                            break;
                        case "COUNT":
                            groupByValues.set(i, currentVal + 1);
                            break;
                        default:
                            groupByValues.set(i, -1);
                            break;
                    }
                }
            }
        }
        // reformat to the format which we want
        // ex: { [2, 4] → [4, 8] } => [2, 4, 4, 8]
        List<Tuple> results = new ArrayList<>();
        for (Map.Entry<List<Integer>, List<Integer>> entry : groups.entrySet()) {
            List<Integer> rowKey = new ArrayList<>(entry.getKey());
            List<Integer> rowValue = new ArrayList<>(entry.getValue());
            rowKey.addAll(rowValue);
            results.add(new Tuple(rowKey));
        }
        return results.iterator();
    }


    @Override
    public void reset() {
        inputSource.reset();
        resultList = null;
    }
}

