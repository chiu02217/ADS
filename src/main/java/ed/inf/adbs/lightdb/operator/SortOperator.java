package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.utils.ColumnHelper;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.IOException;
import java.util.*;

/**
 * class to handle sorting (ORDER BY)
 * if the comments are not clear enough, please refer to README.md
 */
public class SortOperator extends Operator {
    private Operator inputSource;
    private List<OrderByElement> orderByElements;
    private String tableName;

    // sorted tuple
    private List<Tuple> sortedTuples = null;
    private int index = 0;
    private List<String> joinTables;
    // for projection push down (global original index -> index after projection)
    private Map<Integer, Integer> mapping = null;
    // ex: GROUP BY Student.B, Stundent.B's original tuple index is 1, then groupByIndexes contains[1]
    private List<Integer> groupByIndexes;
    public SortOperator(Operator inputSource, List<String> joinTables,
                        List<OrderByElement> orderByElements, List<Integer> groupByIndexs) {
        this.inputSource = inputSource;
        this.joinTables = joinTables;
        this.tableName = joinTables.isEmpty() ? null : joinTables.get(0);
        this.orderByElements = orderByElements;
        this.groupByIndexes = groupByIndexes != null ? groupByIndexs : Collections.emptyList();
    }

    public List<String> getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(List<String> joinTables) {
        this.joinTables = joinTables;
    }

    public Map<Integer, Integer> getMapping() {
        return mapping;
    }

    public void setMapping(Map<Integer, Integer> mapping) {
        this.mapping = mapping;
    }

    public List<Integer> getGroupByIndexes() {
        return groupByIndexes;
    }

    public void setGroupByIndexes(List<Integer> groupByIndexes) {
        this.groupByIndexes = groupByIndexes;
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
        // cautious!!
        if (sortedTuples == null) {
            sort();
        }

        if (index < sortedTuples.size()) {
            return sortedTuples.get(index++);
        }
        return null;
    }
    // Order by
    private void sort() throws IOException{
        sortedTuples = new ArrayList<>();
        Tuple tuple;
        // get data and sort them
        try {
            while ((tuple = inputSource.getNextTuple()) != null) {
                sortedTuples.add(tuple);
            }
            if (orderByElements != null && !orderByElements.isEmpty()) {
                sortedTuples.sort(new TupleComparator());
            }
        }
        catch (Exception e) {
            System.err.println("sort operator error " + e.getMessage());
            throw new IOException("sort operator error");
        }
    }

    @Override
    public void reset() {
        index = 0;
    }
    private int calculateTuplePosition(Column col) {
        int newIndex = ColumnHelper.resolveColumnIndex(col, joinTables, mapping);

        // If GROUP BY，find the slot in the group-by prefix
        if (!groupByIndexes.isEmpty()) {
            for (int i = 0; i < groupByIndexes.size(); i++) {
                if (groupByIndexes.get(i) == newIndex) return i;
            }
        }
        return newIndex;
    }

    /**
     * class to compare two value and then sort
     */
    private class TupleComparator implements Comparator<Tuple> {
        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (OrderByElement element : orderByElements) {
                Column col = (Column) element.getExpression();
                int position = calculateTuplePosition(col);
                int val1 = t1.getKeyValue(position);
                int val2 = t2.getKeyValue(position);
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