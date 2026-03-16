# schema

Manages table metadata loaded from `schema.txt`.

---

## Files

### `Schema.java`

Singleton that loads and exposes schema information for all tables. Initialised once at startup via `Schema.getInstance().initSchema(databaseDir)`.

The schema file format is one table per line:
```
TableName col1 col2 col3 ...
```
Each column is assigned a 0-based index corresponding to its position in the table's CSV file.

| Method | Description |
|--------|-------------|
| `getInstance()` | Returns the singleton instance, creating it on first call. |
| `initSchema(schemaDir)` | Reads `schema.txt` from the given directory and populates the internal `schemas` map: `tableName → { columnName → 0-based index }`. |
| `getColumnIndex(tableName, columnName)` | Returns the 0-based index of a column within its table. Returns `-1` if the table or column is not found. |
| `getTablePath(tableName)` | Returns the absolute file path to a table's CSV data file (`schemaDir/data/TableName.csv`). |
| `getNumberOfTableCol(tableName)` | Returns the column map for a table (`Map<String, Integer>`). Its `.size()` gives the number of columns in the table, used by `ColumnHelper` to compute per-table offsets in joined tuples. |
