package ed.inf.adbs.lightdb.utils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * classs to dealt with WHERE predicate
 */
public class WhereDecomposer {
    /**
     * Per table selection conditions.
     * Key: table name;
     * Value: combined predicate to push down to that table's scan.
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
     * Collects the individual AND-separated predicates from a AND expression tree into a flat list.
     *
     * @param expr
     * @param out   list to accumulate atomic predicates into
     */
    private static void collectPredicates(Expression expr, List<Expression> out) {
        if (expr == null) return;
        if (expr instanceof AndExpression) {
            AndExpression and = (AndExpression) expr;
            // recursive!!
            collectPredicates(and.getLeftExpression(), out);
            collectPredicates(and.getRightExpression(), out);
        }
        else {
            out.add(expr);
        }
    }

    /**
     * Collect the table names which have are referenced by an predicate(WHERE)
     * (if refTables <=1) not Join, (refTables ==2) JOIN Conditions
     * @param expr
     * @return list of distinct table names referenced by this predicate
     */
    private static List<String> getReferencedTables(Expression expr) {
        List<String> refTables = new ArrayList<>();
        ColumnHelper.collectTableNames(expr, refTables);
        return refTables;
    }


    /**
     * Decomposes the WHERE expression and populates tableSelections and joinConditions.
     * Ex: if Where is WHERE Student.A = 5 AND Enrolled.H > 3 AND Student.A = Enrolled.A
     * then : [Student.A = 5,  Enrolled.H > 3,  Student.A = Enrolled.A]
     * @param where   WHERE expression (may be null)
     * @param tables  all tables in FROM order
     */
    public void decompose(Expression where, List<String> tables) {
        if (where == null) return;

        // flatten the AND tree into individual atomic predicates
        List<Expression> predicates = new ArrayList<>();
        collectPredicates(where, predicates);

        // Step 2: classify each predicate
        for (Expression pred : predicates) {
            List<String> refTables = getReferencedTables(pred);

            if (refTables.size() <= 1) {
                // Single table predicate (or constant like 42=42): push down to that table.
                String targetTable = refTables.isEmpty() ? tables.get(0) : refTables.get(0);
                // store single table predicate
                // ex:
                //      tableSelections = {
                //          "Student"  → Student.A = 5
                //          "Enrolled" → Enrolled.H > 3
                //      }
                tableSelections.compute(targetTable,
                        (k, existing) -> existing == null ? pred : new AndExpression(existing, pred));

            }
            else if (refTables.size() == 2) {
                // Join condition: belongs to the join step where the LATER of the two
                String laterTable = getLaterTable(refTables, tables);
                // stor join condition predicate
                // ex:
                //  joinConditions = {
                //    "Enrolled" → Student.A = Enrolled.A
                //    "Course"   → Enrolled.cid = Course.cid
                //  }
                joinConditions.compute(laterTable,
                        (k, existing) -> existing == null ? pred : new AndExpression(existing, pred));

            }
        }
    }

    /**
     * Given two table names, returns the one that appears later in the FROM-order list.
     * This identifies the join step at which both tables are first available together!!
     * Ex:
     *  WHERE R.A = S.B (S is the later table) AND S.C = T.D (D is the later table)
     * @param refs   two table names referenced by a join predicate
     * @param tables all tables in FROM order
     * @return the table name that appears later in tables
     */
    private static String getLaterTable(List<String> refs, List<String> tables) {
        int index0 = tables.indexOf(refs.get(0));
        int index1 = tables.indexOf(refs.get(1));
        return index0 > index1 ? refs.get(0) : refs.get(1);
    }
}
