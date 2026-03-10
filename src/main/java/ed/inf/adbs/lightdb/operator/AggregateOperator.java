package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.utils.Visitor;
import net.sf.jsqlparser.expression.Expression;
import java.util.*;

public class AggregateOperator extends Operator{
    private List<String> aggregationTasks;
    // aggregate by which column(index)
    private List<Integer> aggregationIndexs;
    int columnIndex;
    private Operator inputSource;
    // stored indexs which we use to group by
    private List<Integer> groupByIndex;
    private Iterator<Tuple> resultList;
    private List<Expression> aggregationExpr;
    private Visitor visitor;

    public AggregateOperator(Operator input, List<Integer> groupByIndexs, List<String> aggregationTasks, List<Integer> aggregationIndexs, List<Expression> aggregationExpr, List<String> tables) {
        this.inputSource = input;
        this.groupByIndex = groupByIndexs;
        this.aggregationTasks = aggregationTasks;
        this.aggregationIndexs = aggregationIndexs;
        this.aggregationExpr = aggregationExpr;
        this.visitor = new Visitor();
        this.visitor.setJoinTables(tables);
    }
    private int evaluate(Expression expr, Tuple tuple) {
        visitor.setCurrentTuple(tuple); // 將當前 Tuple 塞給 Visitor
        expr.accept(visitor); // 讓 Visitor 走訪表達式
        return visitor.getCurrentValue(); // 取得計算結果 (例如 H * H 的值)
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

    private Iterator<Tuple> aggregate() {
        Tuple tuple;
        // Use LinkedHashMap to preserve insertion order for deterministic output
        Map<List<Integer>, List<Integer>> groups = new LinkedHashMap<>();

        while ((tuple = inputSource.getNextTuple()) != null) {
            // FIX 1: declare grpupByKeys INSIDE the loop so it is fresh for every row
            List<Integer> groupByKeys = new ArrayList<>();
            for (int idx : groupByIndex) {
                groupByKeys.add(tuple.getKeyValue(idx));
            }

            List<Integer> groupByValues = groups.get(groupByKeys);
            if (groupByValues == null) {
                // First row for this group: initialise aggregation accumulators
                groupByValues = new ArrayList<>();
                for (int i = 0; i < aggregationTasks.size(); i++) {
                    String task = aggregationTasks.get(i).toUpperCase();
                    // FIX 2: use evaluate() to compute the expression value (e.g. H*H)
                    int val = evaluate(aggregationExpr.get(i), tuple);
                    groupByValues.add(task.equals("COUNT") ? 1 : val);
                }
                groups.put(groupByKeys, groupByValues);
            } else {
                // Subsequent rows: update accumulators
                for (int i = 0; i < aggregationTasks.size(); i++) {
                    String task = aggregationTasks.get(i).toUpperCase();
                    // FIX 2: use evaluate() here too, not aggregationIndexs (which is empty)
                    int val = evaluate(aggregationExpr.get(i), tuple);
                    int current = groupByValues.get(i);
                    switch (task) {
                        case "SUM":
                            groupByValues.set(i, current + val);
                            break;
                        case "MIN":
                            groupByValues.set(i, Math.min(current, val));
                            break;
                        case "MAX":
                            groupByValues.set(i, Math.max(current, val));
                            break;
                        case "COUNT":
                            groupByValues.set(i, current + 1);
                            break;
                        default:
                            groupByValues.set(i, -1);
                            break;
                    }
                }
            }
        }

        List<Tuple> results = new ArrayList<>();
        for (Map.Entry<List<Integer>, List<Integer>> entry : groups.entrySet()) {
            List<Integer> finalRow = new ArrayList<>(entry.getKey());
            finalRow.addAll(entry.getValue());
            results.add(new Tuple(finalRow));
        }
        return results.iterator();
    }


    @Override
    public void reset() {
        inputSource.reset();
        resultList = null;
    }
}

