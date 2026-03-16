# operator

All query execution operators. Each operator implements the **iterator model**: callers repeatedly call `getNextTuple()` to pull one result tuple at a time.

---

## Class Hierarchy

```
BaseOperator          (abstract: getNextTuple, reset)
  └─ Operator         (abstract: wraps IOException; exposes protected reader, tablePath)
       ├─ ScanOperator
       ├─ SelectOperator
       ├─ ProjectOperator
       ├─ BlockNestedJoinOperator
       ├─ SortOperator
       ├─ AggregateOperator
       └─ DuplicateEliminationOperator
```

---

## Files

### `BaseOperator.java`

Abstract base defining the operator interface.

| Method | Description |
|--------|-------------|
| `getNextTuple()` | Returns the next result tuple, or `null` when exhausted. |
| `reset()` | Resets the operator to re-read from the beginning. |

---

### `Operator.java`

Intermediate abstract class that catches `IOException` from `_getNextTuple()` and rethrows as `RuntimeException`. Also holds protected fields `reader` (`BufferedReader`) and `tablePath` (`String`) used by `ScanOperator`.

| Method | Description |
|--------|-------------|
| `getNextTuple()` | Calls `_getNextTuple()`; wraps any `IOException`. |
| `_getNextTuple()` | Abstract; implemented by each concrete operator. |

---

### `ScanOperator.java`

Leaf operator. Reads rows from a CSV file one by one.

**Projection push-down:** if a list of local column indices is supplied at construction, the scan returns only those columns from each row instead of the full tuple.

| Constructor | Description |
|-------------|-------------|
| `ScanOperator(tablePath)` | Full scan — returns all columns. |
| `ScanOperator(tablePath, projectedCols)` | Projected scan — returns only the specified local (0-based within-table) column indices. Pass `null` for full scan. |

| Method | Description |
|--------|-------------|
| `_getNextTuple()` | Reads the next CSV line, parses it into a `Tuple`, and applies column projection if `projectedCols` is set. |
| `reset()` | Closes and reopens the file reader from the beginning. |

---

### `SelectOperator.java`

Filters rows based on a predicate (WHERE clause, single-table portion). Wraps any upstream operator.

| Method | Description |
|--------|-------------|
| `_getNextTuple()` | Pulls tuples from upstream and returns the first one that satisfies the predicate. Returns `null` when upstream is exhausted. |
| `getVisitor()` | Returns the internal `Visitor` instance so callers can configure `joinTables` and `mapping` for push-down-aware index lookup. |

---

### `ProjectOperator.java`

Handles column projection. Operates in two modes:

- **Intermediate mode** (`ProjectOperator(inputSource, indexs)`): keeps only the specified global indices from each tuple. Used for projection push-down between join and aggregation.
- **Final mode** (`ProjectOperator(inputSource, joinTables, selectItems, groupByCount)`): maps SELECT-list expressions to tuple values. Handles regular columns, aggregate function results, and GROUP BY output layout.

| Method | Description |
|--------|-------------|
| `_getNextTuple()` | In intermediate mode: extracts the kept indices. In final mode: evaluates each SELECT item against the tuple, accounting for GROUP BY offset and push-down mapping. |
| `setMapping(mapping)` | Sets the push-down index mapping for final-mode column lookups. |

---

### `BlockNestedJoinOperator.java`

Implements **block nested loop join**. Buffers up to 50,000 left-side tuples at a time, then iterates over all right-side tuples for each buffer block.

| Method | Description |
|--------|-------------|
| `_getNextTuple()` | Drives the block-nested-loop. Fills the left buffer, resets the right scan for each block, and pairs every left tuple with every right tuple. Skips pairs that fail the join condition (if any). Returns combined tuples. |
| `fillBuffer()` | Reads up to `_blockSize` tuples from the left source into `leftBuffer`. |
| `combineTuples(left, right)` | Concatenates left and right tuple attribute lists into a single wider tuple. |
| `getVisitor()` | Returns the internal `Visitor` so callers can set `mapping` for scan-projected tuple evaluation. |

---

### `SortOperator.java`

Materialises all upstream tuples and sorts them according to ORDER BY elements. Supports GROUP BY output layout remapping and push-down index mapping.

| Constructor | Description |
|-------------|-------------|
| `SortOperator(inputSource, joinTables, orderByElements, groupByIndexes)` | General constructor. Pass an empty list for `groupByIndexes` when there is no GROUP BY. |

| Method | Description |
|--------|-------------|
| `_getNextTuple()` | Triggers lazy materialisation and sort on first call, then returns tuples sequentially by index. |
| `sort()` | Drains the upstream operator into `sortedTuples`, then sorts using `TupleComparator` if ORDER BY is present. |
| `calculateTuplePosition(col)` | Resolves the tuple index for an ORDER BY column: calls `ColumnHelper.resolveColumnIndex` for push-down mapping, then searches the GROUP BY prefix if applicable. |
| `setMapping(mapping)` | Sets the push-down index mapping propagated from the intermediate projection stage. |

---

### `AggregateOperator.java`

Computes GROUP BY aggregations (SUM, MIN, MAX, COUNT). Materialises all results before returning any.

Output tuple format: `[groupByCol1, groupByCol2, ..., aggResult1, aggResult2, ...]`

| Method | Description |
|--------|-------------|
| `_getNextTuple()` | Triggers lazy aggregation on first call via `aggregate()`, then iterates over results. |
| `aggregate()` | Drains all upstream tuples. Groups them by GROUP BY key values. For each group, applies the aggregation functions (SUM/MIN/MAX/COUNT) incrementally. Returns an iterator over the result tuples. |
| `setMapping(mapping)` | Sets the push-down mapping on the internal `Visitor` so aggregate expressions (e.g. `SUM(S.A)`) correctly look up column values in the post-projection tuple. |

---

### `DuplicateEliminationOperator.java`

Removes duplicate tuples (DISTINCT). Chooses a strategy based on whether the input is already sorted.

| Constructor | Description |
|-------------|-------------|
| `DuplicateEliminationOperator(inputSource, isSorted)` | If `isSorted = true`, uses the sorted strategy (compare consecutive tuples). Otherwise uses a `HashSet`. |

| Method | Description |
|--------|-------------|
| `_getNextTuple()` | **Sorted mode:** returns a tuple only if it differs from the previous one. **Unsorted mode:** returns a tuple only if it has not been seen before (tracked via `HashSet`). |
