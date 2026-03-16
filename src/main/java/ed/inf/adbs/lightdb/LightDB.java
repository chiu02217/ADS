package ed.inf.adbs.lightdb;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import ed.inf.adbs.lightdb.operator.*;
import ed.inf.adbs.lightdb.schema.Schema;
import ed.inf.adbs.lightdb.utils.ColumnHelper;
import ed.inf.adbs.lightdb.utils.WhereDecomposer;
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
		String inputFile = args[1];
		String outputFile = args[2];
		Schema.getInstance().initSchema(args[0]);
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			if (statement instanceof Select) {
				PlainSelect plainSelect = ((Select) statement).getPlainSelect();
				String tableName = plainSelect.getFromItem().toString();
				String tablePath = Schema.getInstance().getTablePath(tableName);
				Operator exePlan = executionPlan(plainSelect, tablePath);
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
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();
			while (tuple != null) {
				writer.write(tuple.toString());
				writer.newLine();
				tuple = root.getNextTuple();
			}
			writer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Scan → Join → intermediate projection → AggregateOperator → SortOperator → Final Projection → Output
	 * @param plainSelect
	 * @param tablePath
	 * @return
	 */
	public static Operator executionPlan(PlainSelect plainSelect, String tablePath) {
		List<String> tables = new ArrayList<>();
		boolean hasAggTask = plainSelect.getSelectItems().stream()
				.anyMatch(item -> item.getExpression() instanceof Function);
		// collect tables' names after FROM
		String firstTable = plainSelect.getFromItem().toString();
		tables.add(firstTable);
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				tables.add(join.getRightItem().toString());
			}
		}

		boolean isSelectAll = plainSelect.getSelectItems().get(0).toString().equals("*");

		// Scan push-down: collect ALL globally referenced columns (SELECT + WHERE + GROUP BY + ORDER BY)
		// so each ScanOperator reads only the columns the query actually needs
		List<Integer> allNeededCols = isSelectAll ? Collections.emptyList()
				: ColumnHelper.collectAllReferencedColumns(plainSelect, tables);
		Map<Integer, Integer> scanMapping = allNeededCols.isEmpty() ? null
				: ColumnHelper.columnMapping(allNeededCols);

		// Decompose WHERE into single-table selections + join conditions
		WhereDecomposer decomposer = new WhereDecomposer();
		decomposer.decompose(plainSelect.getWhere(), tables);

		// scan push-down: first table scan only reads its needed local columns
		List<Integer> firstLocalCols = ColumnHelper.getLocalNeededCols(allNeededCols, firstTable, tables);
		Operator root = new ScanOperator(Schema.getInstance().getTablePath(firstTable),
				firstLocalCols.isEmpty() ? null : firstLocalCols);

		Expression firstTableSelection = decomposer.tableSelections.get(firstTable);
		if (firstTableSelection != null) {
			SelectOperator sel = new SelectOperator(root, firstTableSelection, firstTable);
			// scan push-down: use only this table as context so local index lookup is correct
			sel.getVisitor().setJoinTables(Collections.singletonList(firstTable));
			if (!firstLocalCols.isEmpty()) {
				sel.getVisitor().setMapping(ColumnHelper.columnMapping(firstLocalCols));
			}
			root = sel;
		}
		// if Join condition
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				String innerTable = join.getRightItem().toString();
				// scan push-down: right-side scan only reads its needed local columns
				List<Integer> innerLocalCols = ColumnHelper.getLocalNeededCols(allNeededCols, innerTable, tables);
				Operator rightScan = new ScanOperator(Schema.getInstance().getTablePath(innerTable),
						innerLocalCols.isEmpty() ? null : innerLocalCols);
				Expression innerSel = decomposer.tableSelections.get(innerTable);
				if (innerSel != null) {
					SelectOperator sel = new SelectOperator(rightScan, innerSel, innerTable);
					// scan push-down: use only this table as context so local index lookup is correct
					sel.getVisitor().setJoinTables(Collections.singletonList(innerTable));
					if (!innerLocalCols.isEmpty()) {
						sel.getVisitor().setMapping(ColumnHelper.columnMapping(innerLocalCols));
					}
					rightScan = sel;
				}

				// Join condition for this step (null means cross product)
				Expression joinCond = decomposer.joinConditions.get(innerTable);
				BlockNestedJoinOperator joinOp = new BlockNestedJoinOperator(root, rightScan, joinCond, tables);
				// scan push-down: join evaluates conditions on already scan-projected tuples; apply scan mapping
				if (scanMapping != null) joinOp.getVisitor().setMapping(scanMapping);
				root = joinOp;
			}
		}

		int groupByCount = 0;
		List<Integer> groupByIndexs = Collections.emptyList();
		// Projection push-down: further reduce to only SELECT / GROUP BY / ORDER BY columns
		// (drops any columns that were kept only for WHERE / join-condition evaluation)
		Map<Integer, Integer> mapping = null;
		if (!isSelectAll) {
			List<Integer> neededCols = ColumnHelper.collectNeededColumnsAfterJoin(plainSelect, tables);
			if (!neededCols.isEmpty()) {
				// scan push-down: tuple indices are already compacted by scan projection;
				// translate each neededCol global index to its position in the scan-projected tuple
				List<Integer> intermediateIndexes = new ArrayList<>();
				for (int g : neededCols) {
					intermediateIndexes.add(scanMapping != null
							? ColumnHelper.getValueAfterRemap(scanMapping, g)
							: g);
				}
				root = new ProjectOperator(root, intermediateIndexes);
				mapping = ColumnHelper.columnMapping(neededCols);
			}
		}

		if (plainSelect.getGroupBy() != null || hasAggTask) {
			// getGroupBy maybe null
			if (plainSelect.getGroupBy() != null) {
				groupByIndexs = ColumnHelper.getGroupByColIndexs(plainSelect.getGroupBy().getGroupByExpressionList(), tables);
				// FIX: remap 要在 groupByIndexs 計算完之後才做
				if (mapping != null) {
					final Map<Integer, Integer> map = mapping;
					groupByIndexs = groupByIndexs.stream()
							.map(i -> ColumnHelper.getValueAfterRemap(map, i))
							.collect(Collectors.toList());
				}
				groupByCount = groupByIndexs.size();
			}

			List<String> aggTasks = new ArrayList<>();
			List<Expression> aggExpressions = new ArrayList<>();
			for (SelectItem<?> item : plainSelect.getSelectItems()) {
				Expression expr = item.getExpression();
				if (expr instanceof Function) {
					Function func = (Function) expr;
					aggTasks.add(func.getName().toUpperCase());
					if (func.getParameters() != null) {
						Object obj = func.getParameters().getExpressions().get(0);
						aggExpressions.add((Expression) obj);
					} else {
						aggExpressions.add(new net.sf.jsqlparser.expression.LongValue(1));
					}
				}
			}
			AggregateOperator aggregateOperator = new AggregateOperator(root, groupByIndexs, aggTasks, aggExpressions, tables);
			if (mapping != null) aggregateOperator.setMapping(mapping);
			root = aggregateOperator;
		}

		// if ORDER BY, need SORT
		if (plainSelect.getOrderByElements() != null) {
			SortOperator sortOperator = new SortOperator(root, tables, plainSelect.getOrderByElements(), groupByIndexs);
			if (mapping != null) sortOperator.setMapping(mapping);
			root = sortOperator;
		}

		ProjectOperator projectOperator = new ProjectOperator(root, tables, plainSelect.getSelectItems(), groupByCount);
		if (mapping != null) projectOperator.setMapping(mapping);
		root = projectOperator;

		if (plainSelect.getDistinct() != null) {
			// sorted and unsorted tuples use different distinct methods
			root = new DuplicateEliminationOperator(root, plainSelect.getOrderByElements() != null);
		}

		return root;
	}
}
