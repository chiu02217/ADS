package ed.inf.adbs.lightdb.operator;
import ed.inf.adbs.lightdb.Tuple;
import java.util.HashSet;
import java.util.Set;

/**
 * class to handle DISTINCT
 */
public class DistinctOperator extends Operator {
    private Operator inputSource;
    private Tuple comparedTuple = null;
    private boolean isSorted = false;
    private Set<String> hashList = new HashSet<>();

    public DistinctOperator(Operator inputSource, boolean isSorted) {
        this.inputSource = inputSource;
        this.isSorted = isSorted;
        if (!isSorted) {
            this.hashList = new HashSet<>();
        }
    }
    ///  this is used when the tuple is sorted
    @Override
    protected Tuple _getNextTuple(){
        if (isSorted) {
            return distinctFromSorted();
        }
        return distinctFromUnsorted();

    }
    protected Tuple distinctFromSorted(){
        Tuple currentTuple;
        while ((currentTuple = inputSource.getNextTuple()) != null) {
            if (comparedTuple == null || currentTuple.toString().equals(comparedTuple.toString())) {
                comparedTuple = currentTuple;
                return currentTuple;
            }
        }
        return null;
    }
    ///  hash sorted
    private Tuple distinctFromUnsorted(){
        Tuple currentTuple;
        while ((currentTuple = inputSource.getNextTuple()) != null) {
            String tupleContent = currentTuple.toString();
            if (!hashList.contains(tupleContent)) {
                hashList.add(tupleContent);
                return currentTuple;
            }
        }
        return null;
    }
    @Override
    public void reset() {
        inputSource.reset();
        comparedTuple = null;
    }
}
