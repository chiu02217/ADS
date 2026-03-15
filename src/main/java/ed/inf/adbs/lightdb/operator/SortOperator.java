package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import ed.inf.adbs.lightdb.utils.ColumnChecker;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortOperator extends Operator {
    private Operator inputSource;
    private List<OrderByElement> orderByElements;
    private String tableName;

    // sorted tuple
    private List<Tuple> sortedTuples = null;
    private int index = 0;
    private List<String> joinTables;

    // When sorting after AggregateOperator, this holds the global column indices
    // of the GROUP BY columns in order — so groupByIndexes.get(i) is the global
    // index of the column sitting at position i in the aggregate output tuple.
    // Empty list means no aggregation.
    private List<Integer> groupByIndexes;
    // non aggregation constructor
    public SortOperator(Operator inputSource, String tableName, List<OrderByElement> orderByElements) {
        this.inputSource = inputSource;
        this.tableName = tableName;
        this.orderByElements = orderByElements;
        this.joinTables = Collections.singletonList(tableName);
        this.groupByIndexes = Collections.emptyList();
    }
    // for aggregation constructor
    public SortOperator(Operator inputSource, List<String> joinTables,
                        List<OrderByElement> orderByElements, List<Integer> groupByIndexs) {
        this.inputSource = inputSource;
        this.joinTables = joinTables;
        this.tableName = joinTables.isEmpty() ? null : joinTables.get(0);
        this.orderByElements = orderByElements;
        this.groupByIndexes = groupByIndexes != null ? groupByIndexs : Collections.emptyList();
    }

    public Operator getInputSource() {
        return inputSource;
    }

    public void setInputSource(Operator inputSource) {
        this.inputSource = inputSource;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public void setOrderByElements(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Tuple> getSortedTuples() {
        return sortedTuples;
    }

    public void setSortedTuples(List<Tuple> sortedTuples) {
        this.sortedTuples = sortedTuples;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    protected Tuple _getNextTuple() throws IOException {
        // 1. 延遲加載：只有第一次呼叫時才讀取所有資料並排序
        // cautious!!
        if (sortedTuples == null) {
            getAndSort();
        }

        // 2. 依照排序後的清單逐一回傳
        if (index < sortedTuples.size()) {
            return sortedTuples.get(index++);
        }
        return null;
    }
    /// Order by
    private void getAndSort() throws IOException {
        sortedTuples = new ArrayList<>();
        Tuple tuple;

        // 把下游所有資料吸乾！
        while ((tuple = inputSource.getNextTuple()) != null) {
            sortedTuples.add(tuple);
        }
        if (orderByElements != null && !orderByElements.isEmpty()) {
//            for (OrderByElement element : orderByElements) {
//                ColumnChecker.checkAndGetIndex(element.getExpression(), tableName, "OrderBy Error");
//            }
            sortedTuples.sort(new TupleComparator());
        }
    }

    @Override
    public void reset() {
        index = 0;
    }

    private class TupleComparator implements Comparator<Tuple> {
        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (OrderByElement element : orderByElements) {
                Column col = (Column) element.getExpression();
                //int positionAfterAgg = getColumnIndexAfterJoin(col);
                int val1 = t1.getKeyValue(index);
                int val2 = t2.getKeyValue(index);
                // if same values then skip
                if (val1 != val2) {
                    int result = Integer.compare(val1, val2);
                    // Asc or Desc
                    return element.isAsc() ? result : -result;
                }
            }
            // if val1 == val2
            return 0;
        }
    }

}