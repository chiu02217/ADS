package ed.inf.adbs.lightdb;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import ed.inf.adbs.lightdb.operator.*;
import ed.inf.adbs.lightdb.schema.Schema;
import ed.inf.adbs.lightdb.utils.Visitor;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * Unit tests for LightDB.
 */
public class OperatorTest {
	
	/**z
	 * Rigorous Test :-)
	 */
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private String studentPath;
	private String enrolledPath;

	@Before
	public void setUp() throws Exception {
		String dbDir = temporaryFolder.newFolder("db").getAbsolutePath();
		File dataDir = new File(dbDir + File.separator + "data");
		dataDir.mkdirs();

		try (PrintWriter pw = new PrintWriter(
				new FileWriter(dbDir + File.separator + "schema.txt"))) {
			pw.println("Student A B C");
			pw.println("Enrolled A H");
		}
		// use temp fake data
		studentPath = dataDir.getAbsolutePath() + File.separator + "Student.csv";
		try (PrintWriter pw = new PrintWriter(new FileWriter(studentPath))) {
			pw.println("1, 2, 3");
			pw.println("4, 5, 6");
			pw.println("1, 8, 9");
		}

		enrolledPath = dataDir.getAbsolutePath() + File.separator + "Enrolled.csv";
		try (PrintWriter pw = new PrintWriter(new FileWriter(enrolledPath))) {
			pw.println("1, 10");
			pw.println("4, 20");
			pw.println("2, 30");
		}

		Schema.getInstance().initSchema(dbDir);
	}

	// ── Helpers ─────────────────────────────────────────────────────────────
	// get all tuples
	private List<int[]> drainAll(Operator op) {
		List<int[]> result = new ArrayList<>();
		Tuple tuple;
		while ((tuple = op.getNextTuple()) != null) {
			List<Integer> attrs = tuple.getAttrs();
			int[] row = new int[attrs.size()];
			for (int i = 0; i < attrs.size(); i++) row[i] = attrs.get(i);
			result.add(row);
		}
		return result;
	}
	// parser
	private Expression parseExpr(String sql) throws Exception {
		return CCJSqlParserUtil.parseExpression(sql);
	}
	// for selectOperator tests use
	private SelectOperator makeSelect(Operator src, String exprSql,
									  String table, List<String> tables) throws Exception {
		SelectOperator selectOperator = new SelectOperator(src, parseExpr(exprSql), table);
		selectOperator.getVisitor().setJoinTables(tables);
		return selectOperator;
	}
	// run SQL
	private Operator runQuery(String sql) throws Exception {
		PlainSelect plainSelect = ((Select) CCJSqlParserUtil.parse(sql)).getPlainSelect();
		String first = plainSelect.getFromItem().toString();
		return LightDB.executionPlan(plainSelect, Schema.getInstance().getTablePath(first));
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ScanOperator
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void scan_getNextTuple_firstRow() {
		ScanOperator scan = new ScanOperator(studentPath);
		Tuple tuple = scan.getNextTuple();
		assertNotNull(tuple);
		assertEquals(1, (int) tuple.getKeyValue(0));
		assertEquals(2, (int) tuple.getKeyValue(1));
		assertEquals(3, (int) tuple.getKeyValue(2));
	}

	/**
	 * check return
	 */
	@Test
	public void scan_getNextTuple_rowsReturned() {
		ScanOperator scan = new ScanOperator(studentPath);
		List<int[]> rows = drainAll(scan);
		assertEquals(3, rows.size());
		assertArrayEquals(new int[]{1, 2, 3}, rows.get(0));
		assertArrayEquals(new int[]{4, 5, 6}, rows.get(1));
		assertArrayEquals(new int[]{1, 8, 9}, rows.get(2));
	}

	/**
	 * no return after all data
	 */
	@Test
	public void scan_getNextTuple_returnsNullAfterLastRow() {
		ScanOperator scan = new ScanOperator(studentPath);
		drainAll(scan);
		assertNull(scan.getNextTuple());
	}

	/**
	 * must return null
	 * @throws
	 */
	@Test
	public void scan_getNextTuple_returnsNull() throws Exception {
		File empty = temporaryFolder.newFile("Empty.csv");
		ScanOperator scan = new ScanOperator(empty.getAbsolutePath());
		assertNull(scan.getNextTuple());
	}

	/**
	 * / must restart  from first row
	 */
	@Test
	public void scan_reset_restartsFromFirstRow() {
		ScanOperator scan = new ScanOperator(studentPath);
		String first = scan.getNextTuple().toString();
		drainAll(scan);
		scan.reset();
		assertEquals(first, scan.getNextTuple().toString());
	}

	/**
	 * return only specified needed cols
	 */
	@Test
	public void scan_pushDownConstructor_keepsOnlySpecifiedColumns() {
		// A(0) and C(2) only
		ScanOperator scan = new ScanOperator(studentPath, Arrays.asList(0, 2));
		Tuple tuple = scan.getNextTuple();
		assertNotNull(tuple);
		assertEquals(2, tuple.getAttrs().size());
		assertEquals(1, tuple.getKeyValue(0)); // A=1
		assertEquals(3, tuple.getKeyValue(1)); // C=3
	}

	/**
	 * push down and then reset
	 */
	@Test
	public void scan_pushDownConstructor_reset() {
		ScanOperator scan = new ScanOperator(studentPath, Arrays.asList(0, 2));
		String first = scan.getNextTuple().toString();
		drainAll(scan);
		scan.reset();
		assertEquals(first, scan.getNextTuple().toString());
	}
	// ═══════════════════════════════════════════════════════════════════════
	// SelectOperator
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void selectOperator_getNextTuple_returnOnlyMatchingRows() throws Exception {
		SelectOperator selectOperator = makeSelect(new ScanOperator(studentPath), "Student.A = 1", "Student", Arrays.asList("Student"));
		List<int[]> rows = drainAll(selectOperator);
		assertEquals(2, rows.size());
		assertEquals(1, rows.get(0)[0]);
		assertEquals(1, rows.get(1)[0]);
	}

	@Test
	public void selectOperator_getNextTuple_greaterThan() throws Exception {
		SelectOperator selectOperator = makeSelect(new ScanOperator(studentPath), "Student.B > 4", "Student", Arrays.asList("Student"));
		List<int[]> rows = drainAll(selectOperator);
		assertEquals(2, rows.size()); // B=5 and B=8
	}

	@Test
	public void selectOperator_getNextTuple_noMatchingRows() throws Exception {
		SelectOperator selectOperator = makeSelect(new ScanOperator(studentPath), "Student.A = 99", "Student", Arrays.asList("Student"));
		assertNull(selectOperator.getNextTuple());
	}

	@Test
	public void selectOperator_getNextTuple_falsePredicate() throws Exception {
		SelectOperator selectOperator = makeSelect(new ScanOperator(studentPath), "42 = 43", "Student", Arrays.asList("Student"));
		assertNull(selectOperator.getNextTuple());
	}

	@Test
	public void selectOperator_getNextTuple_predicate() throws Exception {
		SelectOperator selectOperator = makeSelect(new ScanOperator(studentPath), "Student.A = 1 AND Student.B = 2", "Student", Arrays.asList("Student"));
		Tuple tuple = selectOperator.getNextTuple();
		assertNotNull(tuple);
		assertEquals(1, tuple.getKeyValue(0));
		assertEquals(2, tuple.getKeyValue(1));
		// only one tuple
		assertNull(selectOperator.getNextTuple());
	}

	@Test
	public void selectOperator_reset_reIteration() throws Exception {
		SelectOperator selectOperator = makeSelect(new ScanOperator(studentPath), "Student.A = 1", "Student", Arrays.asList("Student"));
		String first = selectOperator.getNextTuple().toString();
		drainAll(selectOperator);
		selectOperator.reset();
		assertEquals(first, selectOperator.getNextTuple().toString());
	}

	// ═══════════════════════════════════════════════════════════════════════
	// ProjectOperator
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void project_getNextTuple_selectOperatorectAll() throws Exception {
		Operator plan = runQuery("SELECT * FROM Student");
		List<int[]> rows = drainAll(plan);
		assertEquals(3, rows.size());
		assertArrayEquals(new int[]{1, 2, 3}, rows.get(0));
	}

	@Test
	public void project_getNextTuple_singleColumn() throws Exception {
		Operator plan = runQuery("SELECT Student.A FROM Student");
		List<int[]> rows = drainAll(plan);
		assertEquals(3, rows.size());
		assertEquals(1, rows.get(0).length); // only 1 column
		assertArrayEquals(new int[]{1}, rows.get(0));
		assertArrayEquals(new int[]{4}, rows.get(1));
	}

	@Test
	public void project_getNextTuple_columnOrder() throws Exception {
		Operator plan = runQuery("SELECT Student.C, Student.A FROM Student");
		List<int[]> rows = drainAll(plan);
		assertArrayEquals(new int[]{3, 1}, rows.get(0)); // C first, then A
	}

	@Test
	public void project_intermediateConstructor_keepsOnlyNeededCols() {
		ProjectOperator proj = new ProjectOperator(new ScanOperator(studentPath), Arrays.asList(0, 2));
		Tuple tuple = proj.getNextTuple();
		assertNotNull(tuple);
		assertEquals(2, tuple.getAttrs().size());
		assertEquals(1, (int) tuple.getKeyValue(0)); // A
		assertEquals(3, (int) tuple.getKeyValue(1)); // C
	}

	@Test
	public void project_reset_restartsFromFirstRow() throws Exception {
		Operator plan = runQuery("SELECT Student.A FROM Student");
		String first = plan.getNextTuple().toString();
		drainAll(plan);
		plan.reset();
		assertEquals(first, plan.getNextTuple().toString());
	}

	// ═══════════════════════════════════════════════════════════════════════
	// SortOperator
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void sort_getNextTuple_ascendingOrder() throws Exception {
		Operator plan = runQuery("SELECT * FROM Student ORDER BY Student.B");
		List<int[]> rows = drainAll(plan);
		assertEquals(3, rows.size());
		// B values must be non-decreasing
		assertTrue(rows.get(0)[1] <= rows.get(1)[1]);
		assertTrue(rows.get(1)[1] <= rows.get(2)[1]);
	}

	@Test
	public void sort_getNextTuple_generalTest() throws Exception {
		Operator plan = runQuery("SELECT * FROM Student ORDER BY Student.A");
		assertEquals(3, drainAll(plan).size());
	}

	@Test
	public void sort_getNextTuple_returnsNull() throws Exception {
		Operator plan = runQuery("SELECT * FROM Student ORDER BY Student.A");
		drainAll(plan);
		assertNull(plan.getNextTuple());
	}

	@Test
	public void sort_reset_rewindsToStartWithoutResorting() throws Exception {
		Operator plan = runQuery("SELECT * FROM Student ORDER BY Student.B");
		String first = plan.getNextTuple().toString();
		drainAll(plan);
		plan.reset();
		assertEquals(first, plan.getNextTuple().toString());
	}

	// ═══════════════════════════════════════════════════════════════════════
	// DuplicateEliminationOperator
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void distinct_unsorted_removesDuplicates() throws Exception {
		Operator plan = runQuery("SELECT DISTINCT Student.A FROM Student");
		// A values: 1, 4, 1 → 2 distinct values
		assertEquals(2, drainAll(plan).size());
	}

	@Test
	public void distinct_unsorted_returnsNull() throws Exception {
		Operator plan = runQuery("SELECT DISTINCT Student.A FROM Student WHERE Student.A = 99999");
		assertNull(plan.getNextTuple());
	}

	@Test
	public void distinct_sorted_skipsConsecutiveDuplicates() throws Exception {
		Operator plan = runQuery("SELECT DISTINCT Student.A FROM Student ORDER BY Student.A");
		List<int[]> rows = drainAll(plan);
		assertEquals(2, rows.size());
		assertEquals(1, rows.get(0)[0]);
		assertEquals(4, rows.get(1)[0]);
	}

	@Test
	public void distinct_reset_fullIterationAgain() throws Exception {
		Operator plan = runQuery("SELECT DISTINCT Student.A FROM Student");
		List<int[]> first = drainAll(plan);
		plan.reset();
		List<int[]> second = drainAll(plan);
		assertEquals(first.size(), second.size());
	}

	// ═══════════════════════════════════════════════════════════════════════
	// AggregateOperator
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void agg_sum_returnSingleRow() throws Exception {
		Operator plan = runQuery("SELECT SUM(Student.B) FROM Student");
		List<int[]> rows = drainAll(plan);
		assertEquals(1, rows.size());
		assertEquals(15, rows.get(0)[0]); // 2+5+8 = 15
	}

	@Test
	public void agg_sum_aggTaskSum() throws Exception {
		Operator plan = runQuery("SELECT SUM(Student.A * Student.B) FROM Student");
		List<int[]> rows = drainAll(plan);
		assertEquals(1, rows.size());
		assertEquals(30, rows.get(0)[0]); // 1*2 +4*5 + 1*8 = 30
	}

	@Test
	public void agg_sum_groupCount() throws Exception {
		Operator plan = runQuery("SELECT Student.A, SUM(Student.B) FROM Student GROUP BY Student.A");
		assertEquals(2, drainAll(plan).size()); // A=1 group, A=4 group
	}

	@Test
	public void agg_sum_sumPerGroup() throws Exception {
		Operator plan = runQuery("SELECT Student.A, SUM(Student.B) FROM Student GROUP BY Student.A");
		List<int[]> rows = drainAll(plan);
		int sumA1 = -1, sumA4 = -1;
		for (int[] row : rows) {
			if (row[0] == 1) sumA1 = row[1]; // B=2+8=10
			if (row[0] == 4) sumA4 = row[1]; // B=5
		}
		assertEquals(10, sumA1);
		assertEquals(5, sumA4);
	}

	@Test
	public void agg_reset_reIteration() throws Exception {
		Operator plan = runQuery("SELECT SUM(Student.B) FROM Student");
		List<int[]> first = drainAll(plan);
		plan.reset();
		List<int[]> second = drainAll(plan);
		assertArrayEquals(first.get(0), second.get(0));
	}

	// ═══════════════════════════════════════════════════════════════════════
	// BlockNestedJoinOperator
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void join_getNextTuple_matchingRows() throws Exception {
		Operator plan = runQuery("SELECT * FROM Student, Enrolled WHERE Student.A = Enrolled.A");
		List<int[]> rows = drainAll(plan);
		assertEquals(3, rows.size());
		assertArrayEquals(new int[]{1, 2, 3, 1, 10}, rows.get(0));
		assertArrayEquals(new int[]{1, 8, 9, 1, 10}, rows.get(1));
		assertArrayEquals(new int[]{4, 5, 6, 4, 20}, rows.get(2));
	}

	@Test
	public void join_getNextTuple_noMatchingRows() throws Exception {
		Operator plan = runQuery("SELECT * FROM Student, Enrolled WHERE Student.A = 99");
		assertNull(plan.getNextTuple());
	}

	@Test
	public void join_getNextTuple_rowCount() throws Exception {
		// 3 Student rows × 3 Enrolled rows = 9
		assertEquals(9, drainAll(runQuery("SELECT * FROM Student, Enrolled")).size());
	}

	@Test
	public void join_getNextTuple_correctWidth() throws Exception {
		Tuple first = runQuery("SELECT * FROM Student, Enrolled").getNextTuple();
		assertNotNull(first);
		assertEquals(5, first.getAttrs().size()); // 3 + 2
	}

	@Test
	public void join_reset_reIteration() throws Exception {
		Operator plan = runQuery(
				"SELECT * FROM Student, Enrolled WHERE Student.A = Enrolled.A");
		String first = plan.getNextTuple().toString();
		drainAll(plan);
		plan.reset();
		assertEquals(first, plan.getNextTuple().toString());
	}

	// ═══════════════════════════════════════════════════════════════════════
	// Visitor
	// ═══════════════════════════════════════════════════════════════════════

	@Test
	public void visitor_evaluate_true() throws Exception {
		Visitor v = new Visitor();
		v.setJoinTables(Arrays.asList("Student"));
		assertTrue(v.evaluate(new Tuple("1, 2, 3"), "Student", parseExpr("Student.A = 1")));
	}

	@Test
	public void visitor_evaluate_false() throws Exception {
		Visitor v = new Visitor();
		v.setJoinTables(Arrays.asList("Student"));
		assertFalse(v.evaluate(new Tuple("4, 5, 6"), "Student", parseExpr("Student.A = 1")));
	}

	@Test
	public void visitor_evaluate_greaterThan() throws Exception {
		Visitor v = new Visitor();
		v.setJoinTables(Arrays.asList("Student"));
		assertTrue(v.evaluate(new Tuple("4, 5, 6"), "Student", parseExpr("Student.B > 4")));
	}

	@Test
	public void visitor_evaluate_andBothTrue() throws Exception {
		Visitor v = new Visitor();
		v.setJoinTables(Arrays.asList("Student"));
		assertTrue(v.evaluate(new Tuple("1, 2, 3"), "Student", parseExpr("Student.A = 1 AND Student.B = 2")));
	}

	@Test
	public void visitor_evaluate_andOneFalse() throws Exception {
		Visitor v = new Visitor();
		v.setJoinTables(Arrays.asList("Student"));
		assertFalse(v.evaluate(new Tuple("1, 2, 3"), "Student", parseExpr("Student.A = 1 AND Student.B = 99")));
	}

	@Test
	public void visitor_evaluate_multiplication() throws Exception {
		Visitor v = new Visitor();
		v.setJoinTables(Arrays.asList("Student"));
		v.setCurrentTuple(new Tuple("3, 4, 5"));
		// ex :  3 * 4 = 12,
		parseExpr("Student.A * Student.B").accept(v);
		assertEquals(12, v.getCurrentValue());
	}
}
