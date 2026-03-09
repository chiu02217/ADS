package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class BlockNestedJoinOperator extends BaseOperator {
    private Operator leftInputSource;
    private Operator rightInputSource;

    // outer table
    private List<Tuple> leftBuffer;
    private int bufferIndex = 0;
    private Tuple currentRightTuple = null;

    private static final int _blockSize = 100;

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

    @Override
    protected Tuple _getNextTuple(){
        while (true) {
            if (leftBuffer.isEmpty()) {
                fillBuffer();
                if (leftBuffer.isEmpty()) return null;

                rightInputSource.reset();
                currentRightTuple = rightInputSource.getNextTuple();
            }

            // 2. 如果右表已經掃描完當前 Block，清空 Buffer 換下一個 Block
            if (currentRightTuple == null) {
                leftBuffer.clear();
                bufferIndex = 0;
                continue;
            }

            // 3. 在目前的 Buffer 中與當前右表 Tuple 進行配對
            if (bufferIndex < leftBuffer.size()) {
                Tuple leftTuple = leftBuffer.get(bufferIndex++);
                return combineTuples(leftTuple, currentRightTuple);
            } else {
                // Buffer 配對完一輪，換右表的下一筆
                bufferIndex = 0;
                currentRightTuple = rightInputSource.getNextTuple();
            }
        }
    }

    /**
     * 一次讀取 blockSize 數量的 Tuple 進入左表 Buffer
     */
    private void fillBuffer(){
        leftBuffer.clear();
        for (int i = 0; i < _blockSize; i++) {
            Tuple t = leftInputSource.getNextTuple();
            if (t == null) break;
            leftBuffer.add(t);
        }
    }

    /**
     * 將左右兩個 Tuple 拼接成一個新的長 Tuple
     */
    private Tuple combineTuples(Tuple left, Tuple right) {
        List<Integer> combinedValues = new ArrayList<>(left.getCols());
        combinedValues.addAll(right.getCols());
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
