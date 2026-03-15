package ed.inf.adbs.lightdb.utils;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    /**
     * return the culumn indexs used to groupby
     * @param groupbyColList
     * @param tables
     * @return
     */
    public List<Integer> getGroupByColIndexs(List<Object> groupbyColList, List<String> tables) {
        List<Integer> indexs = new ArrayList<>();
        for (Object col : groupbyColList) {
            Expression expr = (Expression) col;
            indexs.add(ColumnHelper.getColumnIndexAfterJoin(expr, tables));
        }
        return indexs;
    }
}
