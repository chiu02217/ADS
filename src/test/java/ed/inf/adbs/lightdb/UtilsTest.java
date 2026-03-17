package ed.inf.adbs.lightdb;
import ed.inf.adbs.lightdb.schema.Schema;
import ed.inf.adbs.lightdb.utils.ColumnHelper;
import ed.inf.adbs.lightdb.utils.WhereDecomposer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import static org.junit.Assert.*;


public class UtilsTest {
    @Rule
    public TemporaryFolder temporaryFoldermp = new TemporaryFolder();
    private final List<String> twoTables   = Arrays.asList("Student", "Enrolled");
    private final List<String> threeTables = Arrays.asList("Student", "Enrolled", "Course");

    @Before
    public void setUp() throws Exception {
        String dbDir = temporaryFoldermp.newFolder("db").getAbsolutePath();
        File dataDir = new File(dbDir + File.separator + "data");
        dataDir.mkdirs();
        // temp fake data
        try (PrintWriter pw = new PrintWriter(new FileWriter(dbDir + File.separator + "schema.txt"))) {
            pw.println("Student A B C");
            pw.println("Enrolled A H");
            pw.println("Course cid name");
        }

        // create empty csv files
        for (String t : Arrays.asList("Student", "Enrolled", "Course")) {
            new File(dataDir, t + ".csv").createNewFile();
        }

        Schema.getInstance().initSchema(dbDir);
    }

    // ── helper ──────────────────────────────────────────────────────────────
    private Expression expr(String sql) throws Exception {
        return CCJSqlParserUtil.parseExpression(sql);
    }

    private PlainSelect parse(String sql) throws Exception {
        return ((Select) CCJSqlParserUtil.parse(sql)).getPlainSelect();
    }

    // ──────────────────────────────────────────────────────────────
    // ColumnHelper.getColumnIndexAfterJoin
    //──────────────────────────────────────────────────────────────

    /**
     * First column of the first table → index 0.
     */
    @Test
    public void getColumnIndexAfterJoin_firstTableFirstCol() throws Exception {
        assertEquals(0, ColumnHelper.getColumnIndexAfterJoin(expr("Student.A"), twoTables));
    }

    /** Third column of the first table → index 2.
     *
     */
    @Test
    public void getColumnIndexAfterJoin_firstTableLastCol() throws Exception {
        assertEquals(2, ColumnHelper.getColumnIndexAfterJoin(expr("Student.C"), twoTables));
    }

    /** First column of the second table → offset(Student=3) + 0 = 3.
     *
     */
    @Test
    public void getColumnIndexAfterJoin_secondTableFirstCol() throws Exception {
        assertEquals(3, ColumnHelper.getColumnIndexAfterJoin(expr("Enrolled.A"), twoTables));
    }

    /** Second column of the second table → 3 + 1 = 4.
     *
     */
    @Test
    public void getColumnIndexAfterJoin_secondTableSecondCol() throws Exception {
        assertEquals(4, ColumnHelper.getColumnIndexAfterJoin(expr("Enrolled.H"), twoTables));
    }

    /**
     * First column of the third table → offset(Student=3, Enrolled=2) + 0 = 5.
     */
    @Test
    public void getColumnIndexAfterJoin_thirdTableFirstCol() throws Exception {
        assertEquals(5, ColumnHelper.getColumnIndexAfterJoin(expr("Course.cid"), threeTables));
    }

    /** Non-Column expression throws RuntimeException. */
    @Test(expected = RuntimeException.class)
    public void getColumnIndexAfterJoin_nonColumnExpr() throws Exception {
        ColumnHelper.getColumnIndexAfterJoin(expr("1 + 2"), twoTables);
    }

    // ──────────────────────────────────────────────────────────────
    // ColumnHelper.getGroupByColIndexs
    // ──────────────────────────────────────────────────────────────

    /**
     * GROUP BY a single column returns a one-element list.
     */
    @Test
    public void getGroupByColIndexs_singleColumn() throws Exception {
        PlainSelect plainSelect = parse("SELECT Student.A, SUM(Student.B) FROM Student GROUP BY Student.A");
        List<Integer> idx = ColumnHelper.getGroupByColIndexs(plainSelect.getGroupBy().getGroupByExpressionList(), Arrays.asList("Student"));
        assertEquals(1, idx.size());
        assertEquals(0, (int) idx.get(0)); // A is index 0
    }

    /**
     * GROUP BY two columns returns indexs in the correct order.
     */
    @Test
    public void getGroupByColIndexs_twoColumns() throws Exception {
        PlainSelect plainSelect = parse(
                "SELECT Student.A, Student.B FROM Student GROUP BY Student.A, Student.B");
        List<Integer> idx = ColumnHelper.getGroupByColIndexs(plainSelect.getGroupBy().getGroupByExpressionList(), Arrays.asList("Student"));
        assertEquals(2, idx.size());
        assertEquals(0, (int) idx.get(0)); // A
        assertEquals(1, (int) idx.get(1)); // B
    }

    // ──────────────────────────────────────────────────────────────
    // ColumnHelper.columnMapping
    // ──────────────────────────────────────────────────────────────

    /**
     * Mapping correctly from a list of kept indexs
     */
    @Test
    public void columnMapping_correctMapping() {
        Map<Integer, Integer> mapping = ColumnHelper.columnMapping(Arrays.asList(0, 4));
        assertEquals(2, mapping.size());
        assertEquals(0, (int) mapping.get(0)); // 0 →  0
        assertEquals(1, (int) mapping.get(4)); // 4 →  1
    }

    /**
     * empty mapping test
     */
    @Test
    public void columnMapping_emptyList() {
        assertTrue(ColumnHelper.columnMapping(Collections.emptyList()).isEmpty());
    }

    // ──────────────────────────────────────────────────────────────
    // ColumnHelper.getValueAfterRemap
    //──────────────────────────────────────────────────────────────

    /**
     * Remap a kept index to its new position.
     */
    @Test
    public void getValueAfterRemap_knownIndex() {
        Map<Integer, Integer> mapping = ColumnHelper.columnMapping(Arrays.asList(1, 3));
        assertEquals(0, ColumnHelper.getValueAfterRemap(mapping, 1));
        assertEquals(1, ColumnHelper.getValueAfterRemap(mapping, 3));
    }

    /**
     * Throws when the index was dropped by push-down.
     */
    @Test(expected = RuntimeException.class)
    public void getValueAfterRemap_unknownIndex() {
        Map<Integer, Integer> mapping = ColumnHelper.columnMapping(Arrays.asList(1, 3));
        ColumnHelper.getValueAfterRemap(mapping, 2); // 2 was not kept
    }

    // ──────────────────────────────────────────────────────────────
    // ColumnHelper.resolveColumnIndex
    // ──────────────────────────────────────────────────────────────

    /**
     * no mapping, then returns the original global index.
     */
    @Test
    public void resolveColumnIndex_noMapping() throws Exception {
        assertEquals(1, ColumnHelper.resolveColumnIndex(expr("Student.B"), Arrays.asList("Student"), null));
    }

    /**
     * mapping, then translates original index to compact position.
     */
    @Test
    public void resolveColumnIndex_withMapping() throws Exception {
        // keep only Student.B(1) and Student.C(2) → mapping {1→0, 2→1}
        Map<Integer, Integer> mapping = ColumnHelper.columnMapping(Arrays.asList(1, 2));
        assertEquals(0, ColumnHelper.resolveColumnIndex(expr("Student.B"), Arrays.asList("Student"), mapping));
        assertEquals(1, ColumnHelper.resolveColumnIndex(expr("Student.C"), Arrays.asList("Student"), mapping));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ColumnHelper.collectTableNames
    // ═══════════════════════════════════════════════════════════════════════

    /** Collects a single table name from a column
     *
     */
    @Test
    public void collectTableNames_singleColumn() throws Exception {
        List<String> result = new ArrayList<>();
        ColumnHelper.collectTableNames(expr("Student.A"), result);
        assertEquals(1, result.size());
        assertEquals("Student", result.get(0));
    }

    /**
     * Collects two distinct table names from a join condition
     */
    @Test
    public void collectTableNames_joinCondition() throws Exception {
        List<String> result = new ArrayList<>();
        ColumnHelper.collectTableNames(expr("Student.A = Enrolled.A"), result);
        assertEquals(2, result.size());
        assertTrue(result.contains("Student"));
        assertTrue(result.contains("Enrolled"));
    }

    /**
     * Same table names appears only once
     */
    @Test
    public void collectTableNames_noDuplicates() throws Exception {
        List<String> result = new ArrayList<>();
        ColumnHelper.collectTableNames(expr("Student.A = Student.B"), result);
        assertEquals(1, result.size());
        assertEquals("Student", result.get(0));
    }

    /**
     * no columns then produces empty list
     */
    @Test
    public void collectTableNames_constantExpressionEmpty() throws Exception {
        List<String> result = new ArrayList<>();
        ColumnHelper.collectTableNames(expr("42 = 42"), result);
        assertTrue(result.isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ColumnHelper.collectNeededColumns
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * simple SELECT with two columns collects both indexs.
     */
    @Test
    public void collectNeededColumns_selectTwoColumns() throws Exception {
        PlainSelect plainSelect = parse("SELECT Student.A, Student.C FROM Student");
        List<Integer> cols = ColumnHelper.collectNeededColumns(plainSelect, Arrays.asList("Student"), false);
        assertEquals(2, cols.size());
        assertTrue(cols.contains(0)); // A
        assertTrue(cols.contains(2)); // C
    }

    /**
     * Result is sorted in ascending order
     */
    @Test
    public void collectNeededColumns_resultIsSorted() throws Exception {
        PlainSelect plainSelect = parse("SELECT Student.C, Student.A FROM Student");
        List<Integer> cols = ColumnHelper.collectNeededColumns(plainSelect, Arrays.asList("Student"), false);
        assertEquals(0, (int) cols.get(0)); // A first
        assertEquals(2, (int) cols.get(1)); // C second
    }

    /**
     * GROUP BY column is included even if not in SELECT.
     */
    @Test
    public void collectNeededColumns_includesGroupByCol() throws Exception {
        PlainSelect plainSelect = parse("SELECT Student.A, SUM(Student.B) FROM Student GROUP BY Student.A");
        List<Integer> cols = ColumnHelper.collectNeededColumns(plainSelect, Arrays.asList("Student"), false);
        // A (GROUP BY)
        assertTrue(cols.contains(0));
        // B (SUM)
        assertTrue(cols.contains(1));
    }

    /**
     * ORDER BY column is included
     */
    @Test
    public void collectNeededColumns_includesOrderByCol() throws Exception {
        PlainSelect plainSelect = parse("SELECT Student.A FROM Student ORDER BY Student.B");
        List<Integer> cols = ColumnHelper.collectNeededColumns(plainSelect, Arrays.asList("Student"), false);
        assertTrue(cols.contains(0)); // A (SELECT)
        assertTrue(cols.contains(1)); // B (ORDER BY)
    }

    /**
     * includeWhere=true also collects WHERE columns
     */
    @Test
    public void collectNeededColumns_includesWhereColWhenFlagTrue() throws Exception {
        PlainSelect plainSelect = parse("SELECT Student.A FROM Student WHERE Student.C = 5");
        List<Integer> withWhere = ColumnHelper.collectNeededColumns(plainSelect, Arrays.asList("Student"), true);
        List<Integer> withoutWhere = ColumnHelper.collectNeededColumns(plainSelect, Arrays.asList("Student"), false);
        // flag=true
        assertTrue(withWhere.contains(2));
        // flag=false
        assertFalse(withoutWhere.contains(2));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ColumnHelper.getLocalNeededCols
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Returns local indexs for the first table correctly
     */
    @Test
    public void getLocalNeededCols_firstTable() {
        // global needed: [0(Student.A), 1(Student.B), 4(Enrolled.H)]
        List<Integer> global = Arrays.asList(0, 1, 4);
        List<Integer> local = ColumnHelper.getLocalNeededCols(global, "Student", twoTables);
        assertEquals(2, local.size());
        //A
        assertTrue(local.contains(0));
        //B
        assertTrue(local.contains(1));
    }

    /** Returns local indices for the second table correctly. */
    @Test
    public void getLocalNeededCols_secondTable() {
        // global needed: [0(Student.A), 4(Enrolled.H)]
        List<Integer> global = Arrays.asList(0, 4);
        List<Integer> local = ColumnHelper.getLocalNeededCols(global, "Enrolled", twoTables);
        assertEquals(1, local.size());
        assertEquals(1, (int) local.get(0)); // H is local index 1 in Enrolled
    }

    /** Returns empty list when no columns of that table are needed. */
    @Test
    public void getLocalNeededCols_noColsNeeded() {
        List<Integer> global = Arrays.asList(0); // only Student.A
        List<Integer> local = ColumnHelper.getLocalNeededCols(global, "Enrolled", twoTables);
        assertTrue(local.isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // WhereDecomposer.decompose
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Null WHERE -> empty maps.
     */
    @Test
    public void decompose_nullWhere_emptyMaps() {
        WhereDecomposer whereDecomposer = new WhereDecomposer();
        whereDecomposer.decompose(null, twoTables);
        assertTrue(whereDecomposer.tableSelections.isEmpty());
        assertTrue(whereDecomposer.joinConditions.isEmpty());
    }

    /**
     * Single table predicate goes to tableSelections.
     */
    @Test
    public void decompose_singleTablePredicate_goesToTableSelections() throws Exception {
        WhereDecomposer whereDecomposer = new WhereDecomposer();
        PlainSelect plainSelect = parse("SELECT * FROM Student WHERE Student.A = 5");
        whereDecomposer.decompose(plainSelect.getWhere(), Arrays.asList("Student"));
        assertNotNull(whereDecomposer.tableSelections.get("Student"));
        assertTrue(whereDecomposer.joinConditions.isEmpty());
    }

    /**
     * Join condition goes to joinConditions with the later table as key
     */
    @Test
    public void decompose_joinCondition_goesToJoinConditions() throws Exception {
        WhereDecomposer whereDecomposer = new WhereDecomposer();
        PlainSelect plainSelect = parse("SELECT * FROM Student, Enrolled WHERE Student.A = Enrolled.A");
        whereDecomposer.decompose(plainSelect.getWhere(), twoTables);
        // Enrolled is the later table
        assertNotNull(whereDecomposer.joinConditions.get("Enrolled"));
        assertTrue(whereDecomposer.tableSelections.isEmpty());
    }

    /** WHERE decompsoser, single-table and join conditions are split correctly
     *
     */
    @Test
    public void decompose_mixed_splitCorrectly() throws Exception {
        WhereDecomposer whereDecomposer = new WhereDecomposer();
        PlainSelect plainSelect = parse("SELECT * FROM Student, Enrolled WHERE Student.A = 5 AND Enrolled.H > 3 AND Student.A = Enrolled.A");
        whereDecomposer.decompose(plainSelect.getWhere(), twoTables);
        assertNotNull(whereDecomposer.tableSelections.get("Student"));
        assertNotNull(whereDecomposer.tableSelections.get("Enrolled"));
        assertNotNull(whereDecomposer.joinConditions.get("Enrolled"));
    }

    /** each join condition is assigned to the correct later table
     *
     */
    @Test
    public void decompose_threeTablesJoin_laterTable() throws Exception {
        WhereDecomposer whereDecomposer = new WhereDecomposer();
        PlainSelect plainSelect = parse("SELECT * FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Enrolled.A = Course.cid");
        whereDecomposer.decompose(plainSelect.getWhere(), threeTables);
        // Student.A = Enrolled.A -> Enrolled
        assertNotNull(whereDecomposer.joinConditions.get("Enrolled"));
        // Enrolled.A = Course.cid -> Course
        assertNotNull(whereDecomposer.joinConditions.get("Course"));
    }

    /**
     * Constant predicate (42 = 42) is assigned to the first table.
     */
    @Test
    public void decompose_constantPredicate_assignedToFirstTable() throws Exception {
        WhereDecomposer whereDecomposer = new WhereDecomposer();
        PlainSelect plainSelect = parse("SELECT * FROM Student WHERE 42 = 42");
        whereDecomposer.decompose(plainSelect.getWhere(), Arrays.asList("Student"));
        assertNotNull(whereDecomposer.tableSelections.get("Student"));
    }

    /** Multiple single-table predicates for the same table are ANDed together. */
    @Test
    public void decompose_multiplePredicatesSameTable_andedTogether() throws Exception {
        WhereDecomposer whereDecomposer = new WhereDecomposer();
        PlainSelect plainSelect = parse("SELECT * FROM Student WHERE Student.A = 1 AND Student.B = 2");
        whereDecomposer.decompose(plainSelect.getWhere(), Arrays.asList("Student"));
        assertNotNull(whereDecomposer.tableSelections.get("Student"));
        assertEquals(1, whereDecomposer.tableSelections.size());
    }
}
