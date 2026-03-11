package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import java.util.ArrayList;
import java.util.List;

/**
 * use BlockNested method to join
 */
public class BlockNestedJoinOperator extends Operator {
    private Operator leftInputSource;
    private Operator rightInputSource;

    // for outer table uses
    private List<Tuple> leftBuffer;
    private int bufferIndex = 0;
    private Tuple currentRightTuple = null;
    private static final int _blockSize = 50000;

    public BlockNestedJoinOperator(Operator leftTable, Operator rightTable) {
        this.leftInputSource = leftTable;
        this.rightInputSource = rightTable;
        this.leftBuffer = new ArrayList<>();
        this.reset();
    }

    public Operator getLeftInputSource() {
        return leftInputSource;
    }

    public void setLeftInputSource(Operator leftInputSource) {
        this.leftInputSource = leftInputSource;
    }

    public Tuple getCurrentRightTuple() {
        return currentRightTuple;
    }

    public void setCurrentRightTuple(Tuple currentRightTuple) {
        this.currentRightTuple = currentRightTuple;
    }

    public int getBufferIndex() {
        return bufferIndex;
    }

    public void setBufferIndex(int bufferIndex) {
        this.bufferIndex = bufferIndex;
    }

    public List<Tuple> getLeftBuffer() {
        return leftBuffer;
    }

    public void setLeftBuffer(List<Tuple> leftBuffer) {
        this.leftBuffer = leftBuffer;
    }

    public Operator getRightInputSource() {
        return rightInputSource;
    }

    public void setRightInputSource(Operator rightInputSource) {
        this.rightInputSource = rightInputSource;
    }

    /**
     *
     * @return
     */
    @Override
    protected Tuple _getNextTuple(){
        while (true) {
            if (leftBuffer.isEmpty()) {
                fillBuffer();
                // represent that there is no tuple to compare and join
                if (leftBuffer.isEmpty()) return null;
                rightInputSource.reset();
                currentRightTuple = rightInputSource.getNextTuple();
            }

            // right block is done
            if (currentRightTuple == null) {
                // no right tuple to join, next round(new left buffer round)
                leftBuffer.clear();
                bufferIndex = 0;
                continue;
            }
            // compare and join buffersize left tuples to 1 right tuple
            if (bufferIndex < leftBuffer.size()) {
                Tuple leftTuple = leftBuffer.get(bufferIndex++);
                return combineTuples(leftTuple, currentRightTuple);
            }
            // next right tuple turn
            else {
                bufferIndex = 0;
                currentRightTuple = rightInputSource.getNextTuple();
            }
        }
    }

    /**
     * load tuples into buffer
     */
    private void fillBuffer(){
        leftBuffer.clear();
        for (int i = 0; i < _blockSize; i++) {
            Tuple tuple = leftInputSource.getNextTuple();
            if (tuple == null) break;
            leftBuffer.add(tuple);
        }
    }

    /**
     * left tuple join right tuple into newer larger tuple
     */
    private Tuple combineTuples(Tuple left, Tuple right) {
        List<Integer> combinedValues = new ArrayList<>(left.getAttrs());
        combinedValues.addAll(right.getAttrs());
        return new Tuple(combinedValues);
    }

    @Override
    public void reset() {
        leftInputSource.reset();
        rightInputSource.reset();
        leftBuffer.clear();
        bufferIndex = 0;
        currentRightTuple = null;
    }
}
