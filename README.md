# LightDB

ADS coursework. A lightweight in-memory relational database engine that parses SQL and evaluates queries through a pipeline of operators.

## How I use AI
1. `Clarifying` the concepts of SQL execution and data flow —
   especially in operations like JOIN and GROUP BY, which involve
   changes to column index positions — was crucial. This particularly
   troubled me when moving from Task 1 to Task 2 optimisation, especially when executing query 9-12 that involve more than one column or aggregation tasks such as SUM().
2. When I finished writing the code and was refactoring it, I asked the AI for its opinion.
3. `Helping` me writing some comments more clear
4. `Generating` some unit tests (helped me a lot when finding bugs)
5. `Generating` README.md (ofcourse not this part)

## Usage

```
LightDB <database_dir> <input_file> <output_file>
```

- `database_dir` — directory containing `schema.txt` and a `data/` folder with CSV table files
- `input_file`   — file containing a single SQL SELECT statement
- `output_file`  — file where query results are written (one tuple per line)

## Project Structure

```
src/main/java/ed/inf/adbs/lightdb/
├── LightDB.java          # Entry point; builds and executes the operator tree
├── Tuple.java            # Row representation as List<Integer>
├── operator/             # Pipeline operator implementations (see operator/README.md)
├── schema/               # Schema loading and metadata (see schema/README.md)
└── utils/                # Expression evaluation and column utilities (see utils/README.md)

Besides the main README, there are 3 README files in opeartor/, schema/ and utils/ folders respectively
```

---

## Task 1 — Extracting Join Conditions from the WHERE Clause

The logic is implemented in `utils/WhereDecomposer.java`. Full inline documentation is available there. The following is a summary.

### Approach

Given a WHERE clause such as:
```sql
WHERE Student.A = 5 AND Enrolled.H > 3 AND Student.A = Enrolled.A
```

**Step 1 — Flatten the AND tree (`collectPredicates`)**

JSQLParser represents `A AND B AND C` as a binary tree:
```
      AND
     /   \
    AND   C
   /   \
  A     B
```
`collectPredicates` recurses into `AndExpression` nodes and collects every leaf predicate into a flat list: `[A, B, C]`.

**Step 2 — Classify each predicate (`decompose`)**

For each atomic predicate, `getReferencedTables` walks the expression and collects the distinct table names it references (using `ColumnHelper.collectTableNames`).

| Referenced tables | Classification | Destination |
|---|---|---|
| 0 or 1 | Single-table selection | `tableSelections[tableName]` |
| 2 | Join condition | `joinConditions[laterTable]` |

**Step 3 — Assign join conditions to the correct join step (`getLaterTable`)**

A join condition referencing tables R and S is assigned to whichever appears **later** in the FROM list. This is correct because that is the first join step at which both tables are available in the running left-deep tree.

**Result**

- `tableSelections` — pushed down to a `SelectOperator` directly above each table's `ScanOperator`, filtering rows before any join.
- `joinConditions` — evaluated inside `BlockNestedJoinOperator` when the relevant table is joined in.

---

## Task 2 — Query Optimisation Rules

Two families of push-down optimisations are applied in `LightDB.executionPlan()`.

### 1. Selection Push-Down

**Rule:** A predicate that references only one table is evaluated as early as possible — immediately after that table's scan, before any join.

**How:** `WhereDecomposer` classifies each atomic predicate. Single-table predicates are wrapped in a `SelectOperator` placed directly above the corresponding `ScanOperator`.

**Why it is correct:** A predicate `P(t)` on table `t` depends only on columns of `t`. Its truth value is the same whether evaluated before or after joining with other tables. Evaluating it earlier never changes which rows ultimately satisfy the full WHERE clause.

**Effect on intermediate result size:** Rows that fail the predicate are discarded before they enter any join. Because join size grows multiplicatively (|R| × |S| in the worst case), reducing |R| or |S| early has a proportionally large effect on the join output.

---

### 2. Projection Push-Down (two levels)

Projection push-down keeps only the columns needed to evaluate downstream operators. It is applied at two points in the pipeline.

#### Level 1 — At scan time

**Rule:** Each `ScanOperator` retains only the columns referenced anywhere in the query (SELECT, WHERE, GROUP BY, ORDER BY, join conditions). All unreferenced columns are discarded immediately on read.

**How:** `ColumnHelper.collectNeededColumns(plainSelect, tables, true)` collects all globally-referenced column indices. `ColumnHelper.getLocalNeededCols` translates these to per-table local indices, which are passed to each `ScanOperator`. The scan reads the full CSV row but returns only a `Tuple` containing the projected columns. `SelectOperator` and `BlockNestedJoinOperator` receive a corresponding mapping so they can look up values at the compacted indices.

**Why it is correct:** Any column not referenced anywhere in the query has no effect on correctness. Dropping it at read time produces identical query results.

**Effect:** Every tuple in the system is narrower from the moment it is created. All downstream operators (SELECT, JOIN, AGG, SORT) process fewer integers per tuple.

#### Level 2 — After join (intermediate projection)

**Rule:** After all joins have been processed and WHERE conditions have been evaluated, the tuple is further reduced to only the columns needed by downstream output operators: SELECT items, GROUP BY keys, and ORDER BY keys.

**How:** `ColumnHelper.collectNeededColumns(plainSelect, tables, false)` collects these output-only indices. A `ProjectOperator` (in intermediate mode) is inserted before the aggregation/sort/final-projection stage. A `mapping` (`Map<Integer,Integer>`) is computed and propagated to all subsequent operators so they translate original global column indices to compacted positions.

**Why it is correct:** Columns needed only for WHERE evaluation have already been consumed. The remaining pipeline only needs the output-relevant columns, which are a subset of what the scan already retained.

**Effect:** Aggregation, sorting, and final projection all operate on the smallest possible tuple width, reducing both memory usage and comparison cost.

---

### Combined Pipeline

```
ScanOperator (projection push-down: keep only referenced columns)
  └─ SelectOperator (selection push-down: filter rows before join)
       └─ BlockNestedJoinOperator (join condition evaluation)
            └─ ProjectOperator [intermediate] (projection push-down: drop WHERE-only cols)
                 └─ AggregateOperator
                      └─ SortOperator
                           └─ ProjectOperator [final]
                                └─ DuplicateEliminationOperator
```
