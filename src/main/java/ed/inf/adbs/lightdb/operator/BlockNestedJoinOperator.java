package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.utils.Visitor;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * use BlockNested method to join
 *  * if the comments are not clear enough, please refer to README.md
 */
public class BlockNestedJoinOperator extends Operator {
    private Operator leftInputSource;
    private Operator rightInputSource;

    // for outer table uses
    private List<Tuple> leftBuffer;
    private int bufferIndex = 0;
    private Tuple currentRightTuple = null;
    private static final int _blockSize = 50000;
    private Expression joinCondition;

    // Visitor used to evaluate the join condition on combined tuples
    private Visitor visitor;

    // All table names participating in the query (in FROM order), used by the Visitor
    private List<String> joinTables;

    public BlockNestedJoinOperator(Operator leftTable, Operator rightTable, Expression joinCondition, List<String> joinTables) {
        this.leftInputSource = leftTable;
        this.rightInputSource = rightTable;
        this.joinCondition = joinCondition;
        this.joinTables = joinTables;
        this.visitor = new Visitor();
        this.visitor.setJoinTables(joinTables);
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

    public Expression getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(Expression joinCondition) {
        this.joinCondition = joinCondition;
    }

    public Visitor getVisitor() {
        return visitor;
    }

    public void setVisitor(Visitor visitor) {
        this.visitor = visitor;
    }

    public List<String> getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(List<String> joinTables) {
        this.joinTables = joinTables;
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
                Tuple combined = combineTuples(leftTuple, currentRightTuple);

                // Apply join condition — skip pairs that do not match
                if (joinCondition != null && !visitor.evaluate(combined, joinTables.get(0), joinCondition)) {
                    continue;
                }
                return combined;

            }
            else {
                // Finished pairing the whole buffer with this right tuple; advance right
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
