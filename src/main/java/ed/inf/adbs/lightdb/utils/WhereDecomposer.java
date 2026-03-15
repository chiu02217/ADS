package ed.inf.adbs.lightdb.utils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class WhereDecomposer {

    /**
     * Collects the individual AND-separated predicates from a (possibly nested)
     * AND expression tree into a flat list.
     *
     * @param expr  root of the WHERE expression tree (may be null)
     * @param out   list to accumulate atomic predicates into
     */
    private static void collectPredicates(Expression expr, List<Expression> out) {
        if (expr == null) return;
        if (expr instanceof AndExpression) {
            AndExpression and = (AndExpression) expr;
            collectPredicates(and.getLeftExpression(), out);
            collectPredicates(and.getRightExpression(), out);
        } else {
            out.add(expr);
        }
    }

    /**
     * Determines which table names are referenced by an atomic predicate.
     * Walks the expression tree and collects the table name of every Column node.
     *
     * @param expr    an atomic predicate expression
     * @param tables  all tables participating in the query (in FROM order)
     * @return list of distinct table names referenced by this predicate
     */
    private static List<String> getReferencedTables(Expression expr, List<String> tables) {
        List<String> referenced = new ArrayList<>();
        // Walk all tokens in the expression string representation is fragile;
        // instead use a lightweight visitor to collect Column references.
        collectColumnsFromExpr(expr, tables, referenced);
        return referenced;
    }

    /**
     * Recursively walks an expression to find all Column references and collect
     * the distinct table names they belong to.
     *
     * @param expr       expression to inspect
     * @param allTables  all tables in the query
     * @param result     accumulates distinct table names found
     */
    private static void collectColumnsFromExpr(Expression expr, List<String> allTables,
                                               List<String> result) {
        if (expr instanceof Column) {
            Column col = (Column) expr;
            if (col.getTable() != null && col.getTable().getName() != null) {
                String tName = col.getTable().getName();
                if (!result.contains(tName)) {
                    result.add(tName);
                }
            }
        }
        else if (expr instanceof AndExpression ) {
            collectColumnsFromExpr(((AndExpression) expr).getLeftExpression(), allTables, result);
            collectColumnsFromExpr(((AndExpression) expr).getRightExpression(), allTables, result);
        }
        else if (expr instanceof ComparisonOperator) {
            ComparisonOperator cmp = (ComparisonOperator) expr;
            collectColumnsFromExpr(cmp.getLeftExpression(), allTables, result);
            collectColumnsFromExpr(cmp.getRightExpression(), allTables, result);
        }
        else if (expr instanceof Multiplication) {
            Multiplication mul = (Multiplication) expr;
            collectColumnsFromExpr(mul.getLeftExpression(), allTables, result);
            collectColumnsFromExpr(mul.getRightExpression(), allTables, result);
        }
        else if (expr instanceof Addition) {
            Addition add = (Addition) expr;
            collectColumnsFromExpr(add.getLeftExpression(), allTables, result);
            collectColumnsFromExpr(add.getRightExpression(), allTables, result);
        }
    }

    /**
     * Combines a list of predicates back into a single AND-chained Expression,
     * or returns null if the list is empty.
     *
     * @param predicates list of atomic predicates to combine
     * @return combined expression, or null if predicates is empty
     */
    private static Expression combineWithAnd(List<Expression> predicates) {
        if (predicates.isEmpty()) return null;
        Expression result = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            result = new AndExpression(result, predicates.get(i));
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // Public result holders
    // -----------------------------------------------------------------------

    /**
     * Per-table selection conditions.
     * Key: table name; Value: combined predicate to push down to that table's scan.
     */
    public final Map<String, Expression> tableSelections = new HashMap<>();

    /**
     * Join conditions for each join step.
     * joinConditions.get(i) is the condition to apply when joining the (i+1)-th
     * table (tables.get(i+1)) into the running left-deep join tree.
     * Index 0 corresponds to the join of tables[0] with tables[1].
     */
    public final Map<String, Expression> joinConditions = new HashMap<>();

    /**
     * Decomposes the WHERE expression and populates tableSelections and joinConditions.
     *
     * @param where   the WHERE clause expression (may be null)
     * @param tables  all tables in FROM order
     */
    public void decompose(Expression where, List<String> tables) {
        if (where == null) return;

        // Step 1: flatten the AND tree into individual atomic predicates
        List<Expression> predicates = new ArrayList<>();
        collectPredicates(where, predicates);

        // Step 2: classify each predicate
        for (Expression pred : predicates) {
            List<String> refs = getReferencedTables(pred, tables);

            if (refs.size() <= 1) {
                // Single-table predicate (or constant like 42=42): push down to that table.
                // If no table is referenced (constant), assign to the first table.
                String targetTable = refs.isEmpty() ? tables.get(0) : refs.get(0);
                tableSelections.compute(targetTable,
                        (k, existing) -> existing == null ? pred : new AndExpression(existing, pred));

            }
            else if (refs.size() == 2) {
                // Join condition: belongs to the join step where the LATER of the two
                // tables first appears in the FROM list.
                String laterTable = getLaterTable(refs, tables);
                joinConditions.compute(laterTable,
                        (k, existing) -> existing == null ? pred : new AndExpression(existing, pred));

            }
            // Predicates involving 3+ tables are rare in valid input (cross-product queries).
            // We fall through and let them be handled by a post-join SelectOperator if needed.
        }
    }

    /**
     * Given two table names, returns the one that appears later in the FROM-order list.
     * This identifies the join step at which both tables are first available together.
     *
     * @param refs   two table names referenced by a join predicate
     * @param tables all tables in FROM order
     * @return the table name that appears later in tables
     */
    private static String getLaterTable(List<String> refs, List<String> tables) {
        int idx0 = tables.indexOf(refs.get(0));
        int idx1 = tables.indexOf(refs.get(1));
        return idx0 > idx1 ? refs.get(0) : refs.get(1);
    }
}
