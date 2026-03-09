package ed.inf.adbs.lightdb.utils;

import ed.inf.adbs.lightdb.schema.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    // 1. 解析 GROUP BY 的欄位索引
    public List<Integer> parseGroupByIndexs(PlainSelect plainSelect, List<String> tables) {
        List<Integer> indices = new ArrayList<>();
        if (plainSelect.getGroupBy() != null) {
            for (Object obj : plainSelect.getGroupBy().getGroupByExpressionList()) {
                // 使用 getColumnIndex 邏輯轉換為數字
                Expression expr = (Expression) obj;
                indices.add(calculateColumnGlobalIndex(expr, tables));
            }
        }
        return indices;
    }

    // 2. 解析聚合函數名稱 (SUM, COUNT...)
    public List<String> parseAggTasks(PlainSelect plainSelect) {
        List<String> functions = new ArrayList<>();
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof Function) {
                functions.add(((Function) expr).getName().toUpperCase());
            }
        }
        return functions;
    }

    // 3. 解析聚合函數內部的欄位索引
    public List<Integer> parseAggColumnIndexs(PlainSelect plainSelect, List<String> tables) {
        List<Integer> indices = new ArrayList<>();
        for (SelectItem<?> item : plainSelect.getSelectItems()) {
            Expression expr = item.getExpression();
            if (expr instanceof Function) {
                Function f = (Function) expr;
                // 处理 COUNT(*)
                if (f.isAllColumns()) {
                    indices.add(-1);   // 自己定义 -1 代表 *
                    continue;
                }
                // 拿到 SUM(A) / COUNT(A) 里的 A
                Object obj = f.getParameters().getExpressions().get(0);
                Expression expr2 = (Expression) obj;
                indices.add(calculateColumnGlobalIndex(expr2, tables));
            }
        }
        return indices;
    }

    private static int calculateColumnGlobalIndex(Expression expr, List<String> tables) {
        if (expr instanceof net.sf.jsqlparser.schema.Column) {
            net.sf.jsqlparser.schema.Column col = (net.sf.jsqlparser.schema.Column) expr;

            // 處理表名：若 SQL 沒寫，預設為 FROM 的第一張表
            String tableName = (col.getTable() != null && col.getTable().getName() != null)
                    ? col.getTable().getName()
                    : tables.get(0);
            String columnName = col.getColumnName();

            int offset = 0;
            for (String t : tables) {
                if (t.equals(tableName)) {
                    // 找到目標表，回傳：累加的位移 + 欄位在該表內的索引
                    return offset + Schema.getInstance().getColumnIndex(t, columnName);
                }
                // 關鍵點：使用你現有的 getNumberOfTableCol(t).size() 拿到該表欄位總數
                offset += Schema.getInstance().getNumberOfTableCol(t).size();
            }
        }
        // 如果找不到欄位，拋出異常以便除錯
        throw new RuntimeException("Column not found in joined tables: " + expr);
    }
}
