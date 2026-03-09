package ed.inf.adbs.lightdb.operator;

import ed.inf.adbs.lightdb.Tuple;
import ed.inf.adbs.lightdb.utils.Visitor;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;

public class SelectOperator extends BaseOperator{
    private Operator inputSource;
    private Expression expression;


    private Visitor visitor;
    private String tableName;

    public SelectOperator(Operator inputSource, Expression expression, String tableName) {
        this.inputSource = inputSource;
        this.expression = expression;
        this.tableName = tableName;
        this.visitor = new Visitor();
    }
    public Visitor getVisitor() {
        return this.visitor;
    }
    public Expression getExpression() {
        return expression;
    }

    public Operator getInputSource() {
        return inputSource;
    }

    public String getTableName() {
        return tableName;
    }

    public void setInputSource(Operator inputSource) {
        this.inputSource = inputSource;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setVisitor(Visitor visitor) {
        this.visitor = visitor;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    protected Tuple _getNextTuple() throws IOException {
        try {
            Tuple tuple;
            // get next (true) tuple
            while ((tuple = inputSource.getNextTuple()) != null) {
                if (evaluate(tuple, expression)) {
                    return tuple;
                }
            }
        } catch (Exception e) {
            System.err.println("select operator error " + e.getMessage());
            throw new IOException("select operator error");
        }
        return null;
    }

    @Override
    public void reset() {
        inputSource.reset();
    }

    private boolean evaluate(Tuple tuple, Expression expr) {
        if (expr != null)
        {
            return visitor.evaluate(tuple, tableName, expr);
        }
        return true;
    }
}
