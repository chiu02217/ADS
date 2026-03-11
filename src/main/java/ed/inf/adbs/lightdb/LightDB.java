package ed.inf.adbs.lightdb;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ed.inf.adbs.lightdb.operator.*;
import ed.inf.adbs.lightdb.schema.Schema;
import ed.inf.adbs.lightdb.utils.Parser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Lightweight in-memory database system.
 *
 * Feel free to modify/move the provided functions. However, you must keep
 * the existing command-line interface, which consists of three arguments.
 *
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}
		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];
		// load schema when starting the program
		Schema.getInstance().initSchema(args[0]);
		// Just for demonstration, replace this function call with your logic
		//parsingExample(inputFile);
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			if (statement instanceof Select) {
				PlainSelect plainSelect = ((Select) statement).getPlainSelect();
				String tableName = plainSelect.getFromItem().toString();
				String tablePath = Schema.getInstance().getTablePath(tableName);
				Operator exePlan = executionPlan(plainSelect, tablePath);
//				Tuple tuple = exePlan.getNextTuple();
//				while (tuple  != null) {
//					System.out.println(tuple);
//					tuple = exePlan.getNextTuple();
//				}

				// exe and output
				execute(exePlan, outputFile);
			}
		}
		catch (Exception e){
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement
	 * from a file or a string and prints the SELECT and WHERE clauses to screen.
	 */

	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
//            Statement statement = CCJSqlParserUtil.parse("SELECT Course.cid, Student.name FROM Course, Student WHERE Student.sid = 3");
			if (statement != null) {
				Select select = (Select) statement;
				System.out.println("Statement: " + select);
				System.out.println("SELECT items: " + select.getPlainSelect().getSelectItems());
				System.out.println("WHERE expression: " + select.getPlainSelect().getWhere());
				System.out.println("Order by " + select.getPlainSelect().getOrderByElements());
				System.out.println("Group by " + select.getPlainSelect().getGroupBy().getGroupByExpressionList());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 *
	 * @param root The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try {
			// Create a BufferedWriter
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();
			while (tuple != null) {
				writer.write(tuple.toString());
				writer.newLine();
				tuple = root.getNextTuple();
			}

			// Close the writer
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static Operator executionPlan(PlainSelect plainSelect, String tablePath) {
		List<String> tables = new ArrayList<>();
		boolean hasAggTask = plainSelect.getSelectItems().stream()
				.anyMatch(item -> item.getExpression() instanceof Function);
		// scan
		String firstTable = plainSelect.getFromItem().toString();
		tables.add(firstTable);
		Operator root = new ScanOperator(firstTable, Schema.getInstance().getTablePath(firstTable));

		// Join
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				String innerTable = join.getRightItem().toString();
				tables.add(innerTable);
				Operator rightScan = new ScanOperator(innerTable, Schema.getInstance().getTablePath(innerTable));
				root = new BlockNestedJoinOperator(root, rightScan);
			}
		}

		// WHERE 條件
		if (plainSelect.getWhere() != null) {
			SelectOperator sp = new SelectOperator(root, plainSelect.getWhere(), firstTable);
			sp.getVisitor().setJoinTables(tables);
			root = sp;
		}
		int groupByCount = 0;
		List<Integer> groupByIndexs = Collections.emptyList(); // 宣告移到外面，預設空列表
		if (plainSelect.getGroupBy() != null || hasAggTask) {
			Parser parser = new Parser();
			groupByIndexs = parser.getGroupByColIndexs(plainSelect.getGroupBy().getGroupByExpressionList(), tables);
			groupByCount = groupByIndexs.size(); // <-- FIX: capture size

			List<String> aggTasks = new ArrayList<>();
			List<Expression> aggExpressions = new ArrayList<>();
			for (SelectItem<?> item : plainSelect.getSelectItems()) {
				Expression expr = item.getExpression();
				if (expr instanceof Function) {
					Function f = (Function) expr;
					aggTasks.add(f.getName().toUpperCase());
					if (f.getParameters() != null) {
						Object obj = f.getParameters().getExpressions().get(0);
						aggExpressions.add((Expression) obj);
					} else {
						aggExpressions.add(new net.sf.jsqlparser.expression.LongValue(1));
					}
				}
			}
			root = new AggregateOperator(root, groupByIndexs, aggTasks, aggExpressions, tables);
		}

		// 3. 如果有 ORDER BY，加上 SortOperator
		if (plainSelect.getOrderByElements() != null) {
			root = new SortOperator(root, tables, plainSelect.getOrderByElements(), groupByIndexs);
		}
		// 4. 最後加上 ProjectOperator (處理 SELECT 欄位)
		root = new ProjectOperator(root, tables, plainSelect.getSelectItems(), groupByCount);
		if (plainSelect.getDistinct() != null) {
			// sorted and unsorted tuples use different distinct methods
			root = new DistinctOperator(root,plainSelect.getOrderByElements() != null);
		}


		return root;
	}
}
