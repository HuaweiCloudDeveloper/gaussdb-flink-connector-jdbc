package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/** Test batch mode DELETE FROM using TableEnvironment in batch mode + RowLevelDelete. */
public class DeleteBatchEndToEndTest extends AbstractTestBase {

    private static final String URL = "jdbc:gaussdb://1.92.120.69:8000/postgres";
    private static final String USER = "root";
    private static final String PASS = "GaussDB123";

    @Test
    public void testBatchDelete() throws Exception {
        // Step 1: Prepare table with data
        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS batch_delete_test");
            s.execute(
                    "CREATE TABLE batch_delete_test (id INT PRIMARY KEY, name VARCHAR(100), val INT)");
            s.execute("INSERT INTO batch_delete_test VALUES (1, 'Alice', 100)");
            s.execute("INSERT INTO batch_delete_test VALUES (2, 'Bob', 200)");
            s.execute("INSERT INTO batch_delete_test VALUES (3, 'Charlie', 300)");
        }
        System.out.println("[PREP] Table ready: 3 rows.");

        // Step 2: Create batch TableEnvironment
        org.apache.flink.table.api.TableEnvironment bEnv =
                org.apache.flink.table.api.TableEnvironment.create(
                        EnvironmentSettings.inBatchMode());

        // Register the source table (reads from GaussDB)
        bEnv.executeSql(
                String.format(
                        "CREATE TABLE batch_src ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  val INT,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'gaussdb',"
                                + "  'url' = '%s',"
                                + "  'table-name' = 'batch_delete_test',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s'"
                                + ")",
                        URL, USER, PASS));

        // Also register the sink (writes to same GaussDB table)
        bEnv.executeSql(
                String.format(
                        "CREATE TABLE batch_sink ("
                                + "  id INT,"
                                + "  name STRING,"
                                + "  val INT,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'gaussdb',"
                                + "  'url' = '%s',"
                                + "  'table-name' = 'batch_delete_test',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'sink.buffer-flush.max-rows' = '1',"
                                + "  'sink.buffer-flush.interval' = '0'"
                                + ")",
                        URL, USER, PASS));

        // Step 3: Execute DELETE FROM in batch mode
        System.out.println("[BATCH] Executing DELETE FROM batch_sink WHERE id = 2...");
        bEnv.executeSql("DELETE FROM batch_sink WHERE id = 2").await();
        System.out.println("[BATCH] Job completed.");

        // Step 4: Verify
        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM batch_delete_test ORDER BY id")) {

            System.out.println("\n=== Final table contents ===");
            java.util.List<String> names = new java.util.ArrayList<>();
            int count = 0;
            while (rs.next()) {
                count++;
                String name = rs.getString("name");
                names.add(name);
                System.out.println(
                        String.format(
                                "  id=%d  name=%s  val=%d",
                                rs.getInt("id"), name, rs.getInt("val")));
            }

            if (count == 2
                    && !names.contains("Bob")
                    && names.contains("Alice")
                    && names.contains("Charlie")) {
                System.out.println("\n✅ BATCH DELETE WORKS!");
            } else {
                System.out.println(
                        "\n❌ FAIL: Expected 2 (Alice,Charlie), got " + count + ": " + names);
                throw new AssertionError("Batch DELETE failed: " + names);
            }
        }

        // Cleanup
        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS batch_delete_test");
        }
    }
}
