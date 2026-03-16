# utils

Utility classes shared across the operator pipeline. Handles expression evaluation, column index arithmetic, and WHERE clause decomposition.

---

## Files

### `Visitor.java`

Evaluates a JSQLParser `Expression` tree against a `Tuple` using the **Visitor pattern** (extends `ExpressionVisitorAdapter`). Used by `SelectOperator`, `BlockNestedJoinOperator`, and `AggregateOperator`.

| Method | Description |
|--------|-------------|
| `evaluate(tuple, tableName, expr)` | Entry point. Sets the current tuple and triggers `expr.accept(this)`. Returns the boolean result after traversal. |
| `visit(LongValue)` | Leaf node. Stores the integer literal in `currentValue`. |
| `visit(Column)` | Leaf node. Calls `ColumnHelper.resolveColumnIndex` to translate the column reference to a tuple index (applying push-down mapping if set), then reads `currentTuple.getKeyValue(index)` into `currentValue`. |
| `visitComparison(expr, BiPredicate)` | Private helper shared by all six comparison visitors. Evaluates left and right sub-expressions, then applies the provided predicate to set `currentResult`. |
| `visit(GreaterThan/MinorThan/EqualsTo/NotEqualsTo/GreaterThanEquals/MinorThanEquals)` | Each delegates to `visitComparison` with the corresponding lambda `(l, r) -> l OP r`. |
| `visit(AndExpression)` | Evaluates both sides; sets `currentResult = left && right`. No short-circuit (both sides always evaluated). |
| `visit(OrExpression)` | Evaluates left side; if true, returns immediately (short-circuit). Otherwise evaluates right side. |
| `visit(Multiplication)` | Evaluates both sides; sets `currentValue = left * right`. |
| `visit(Addition)` | Evaluates both sides; sets `currentValue = left + right`. |
| `setJoinTables(tables)` | Sets the table list used by `ColumnHelper.resolveColumnIndex` for global index computation. |
| `setMapping(mapping)` | Sets the projection push-down mapping (`original global index → compact index`). Used after projection push-down is applied at scan time or via the intermediate `ProjectOperator`. |

---

### `ColumnHelper.java`

Static utility methods for column index arithmetic and collection. All index values are **0-based global indices** into the concatenated joined tuple, unless noted as "local".

| Method | Description |
|--------|-------------|
| `getColumnIndexAfterJoin(expr, tables)` | Given a `Column` expression and the ordered list of tables, computes the column's absolute (global) index in the joined tuple. Accounts for per-table column offsets. Falls back to `tables.get(0)` if the column has no table qualifier (supports unqualified column names in single-table queries). |
| `getGroupByColIndexs(groupByColList, tables)` | Converts a list of GROUP BY expressions to their global index positions by calling `getColumnIndexAfterJoin` for each. |
| `columnMapping(indexs)` | Builds a `Map<Integer,Integer>` from a sorted list of needed global indices to their new compact positions (0, 1, 2, …). Used to create the push-down remapping table. Example: `[0, 4] → {0→0, 4→1}`. |
| `resolveColumnIndex(expr, tables, mapping)` | Combines `getColumnIndexAfterJoin` and `getValueAfterRemap` into a single call. Returns the tuple index to use when reading a column, applying the mapping if present. |
| `getValueAfterRemap(mapping, originalIndex)` | Looks up an original global index in a mapping produced by `columnMapping`. Throws `RuntimeException` if the index is not found. |
| `collectTableNames(expr, result)` | Recursively walks an expression and accumulates the distinct table names of all `Column` references found. Used by `WhereDecomposer` to determine which tables a predicate references. |
| `collectNeededColumns(plainSelect, tables, includeWhere)` | Collects all globally-referenced column indices from the query. `includeWhere = true` includes WHERE columns (used for projection push-down at scan time, to ensure selection push-down can still evaluate predicates); `includeWhere = false` collects only SELECT / GROUP BY / ORDER BY columns (used to build the intermediate projection mapping). Returns a sorted list. |
| `getLocalNeededCols(globalNeededCols, tableName, tables)` | Given a list of global needed indices and a target table, returns the within-table (local, 0-based) indices for that table. Used to configure each `ScanOperator` for projection push-down. |
| `collectTableNames` (private helper `walkColumns`) | Recursively traverses an expression tree and calls a callback for every `Column` node found. Handles `Column`, `Multiplication`, `Addition`, `ComparisonOperator`, `AndExpression`, `OrExpression`. |

---

### `WhereDecomposer.java`

Decomposes a WHERE expression into per-table selection conditions and join conditions, enabling selection push-down (apply each selection as soon as its referenced columns are available).

**Public fields**

| Field | Type | Description |
|-------|------|-------------|
| `tableSelections` | `Map<String, Expression>` | Per-table predicates to push down to each `SelectOperator`. Key = table name. |
| `joinConditions` | `Map<String, Expression>` | Join predicates keyed by the later table in FROM order. |

| Method | Description |
|--------|-------------|
| `decompose(where, tables)` | Entry point. Flattens the WHERE AND-tree with `collectPredicates`, then classifies each atomic predicate. Single-table predicates go to `tableSelections`; two-table predicates go to `joinConditions`. Multiple predicates for the same table/join are combined with `AND`. |
| `collectPredicates(expr, out)` | Private. Recursively traverses `AndExpression` nodes and collects all non-AND leaf predicates into a flat list. |
| `getReferencedTables(expr)` | Private. Returns the distinct table names referenced by a predicate by calling `ColumnHelper.collectTableNames`. The count determines whether the predicate is a selection (≤ 1 table) or a join condition (2 tables). |
| `getLaterTable(refs, tables)` | Private. Given two table names from a join predicate, returns the one that appears later in the FROM-order list. A join condition is assigned to this table because it is the first join step at which both tables are simultaneously available. |
