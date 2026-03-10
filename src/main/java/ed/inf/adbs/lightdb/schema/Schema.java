package ed.inf.adbs.lightdb.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * class to handle schema
 */
public final class Schema {
    // singleton
    private static Schema INSTANCE;
    private Map<String, Map<String, Integer>> schemas = new HashMap<>();
    private String schemaDir;



    private Schema(){}

    public static Schema getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new Schema();
        }
        return INSTANCE;
    }

    /**
     * init the schema based on the user input(but the file name must be "schema.txt")
     * ex :
     * {
     *   "Student"  → { "A"=0, "B"=1, "C"=2 }
     *   "Enrolled" → { "A"=0, "H"=1 }
     *   "Course"   → { "E"=0, "F"=1, "G"=2}
     * }
     * @param schemaDir
     *
     */
    public void initSchema(String schemaDir) {
        this.schemaDir = schemaDir;
        try {
            File schema = new File(schemaDir + File.separator + "schema.txt");
            BufferedReader bReader = new BufferedReader(new FileReader(schema));
            String row;
            while ((row = bReader.readLine()) != null) {
                String[] table = row.split("\\s+");
                Map<String, Integer> columnMap = new HashMap<>();
                // from 1 because the column starts from 1 (0 is tableName)
                for (int i = 1; i < table.length; i++) {
                    columnMap.put(table[i], i - 1);
                }
                // table[0] == table name
                schemas.put(table[0], columnMap);
            }
            bReader.close();
        } catch (Exception e) {
            System.err.println("Schema loading error: " + e.getMessage());
        }
    }

    /**
     * get the column's index of the specific col
     * not put in the ColumnChecker because it needs the schema instance
     * @param tableName
     * @param columnName
     * @return
     */
    public int getColumnIndex(String tableName, String columnName) {
        Map<String, Integer> table = schemas.get(tableName);
        if (table != null && table.containsKey(columnName)) {
            return table.get(columnName);
        }
        // don't have this table
        return -1;
    }

    /**
     * get table file(csv) path
     * @param tableName
     * @return
     */
    public String getTablePath(String tableName) {
        if (schemaDir == null) {
            throw new IllegalStateException("Schema not initialized.");
        }
        return schemaDir + File.separator + "data" + File.separator + tableName + ".csv";
    }

    /**
     * get the quantity of the table's cols
     * @param tableName
     * @return
     */
    public Map<String, Integer> getNumberOfTableCol(String tableName){
        if (!schemas.containsKey(tableName)) {
            throw new RuntimeException("Table " + tableName + " not found");
        }
        return schemas.get(tableName);
    }
}
