package ed.inf.adbs.lightdb.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public final class Schema {
    private static Schema INSTANCE;
    private Map<String, Map<String, Integer>> schemas = new HashMap<>();
    private String databaseDir;



    private Schema(){}

    public static Schema getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new Schema();
        }
        return INSTANCE;
    }

    /// init schema to
    ///
    public void initSchema(String databaseDir) {
        this.databaseDir = databaseDir;
        try {
            File schema = new File(databaseDir + File.separator + "schema.txt");
            BufferedReader bReader = new BufferedReader(new FileReader(schema));
            String row;
            while ((row = bReader.readLine()) != null) {
                String[] table = row.split("\\s+");
                Map<String, Integer> columnMap = new HashMap<>();
                for (int i = 1; i < table.length; i++) {
                    columnMap.put(table[i], i - 1); // 欄位名稱 -> 0, 1, 2...
                }
                // table[0] == table name
                schemas.put(table[0], columnMap);
            }
            bReader.close();
        } catch (Exception e) {
            System.err.println("Schema loading error: " + e.getMessage());
        }
    }
    public int getColumnIndex(String tableName, String columnName) {
        Map<String, Integer> table = schemas.get(tableName);
        if (table != null && table.containsKey(columnName)) {
            return table.get(columnName);
        }
        // no this table
        return -1;
    }
    public String getDataPath(String tableName) {
        if (databaseDir == null) {
            throw new IllegalStateException("Schema not initialized.");
        }
        return databaseDir + File.separator + "data" + File.separator + tableName + ".csv";
    }
    public Map<String, Integer> getNumberOfTableCol(String tableName){
        if (!schemas.containsKey(tableName)) {
            throw new RuntimeException("Table " + tableName + " not found");
        }
        return schemas.get(tableName);
    }
}
