package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/** Single-table batch DELETE test (matches sql-client scenario). */
public class DeleteSingleTableTest extends AbstractTestBase {

    private static final String URL = "jdbc:gaussdb://1.92.120.69:8000/postgres";
    private static final String USER = "root";
    private static final String PASS = "GaussDB123";

    @Test
    public void testSingleTableBatchDelete() throws Exception {
        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS single_delete_test");
            s.execute(
                    "CREATE TABLE single_delete_test (id INT PRIMARY KEY, name VARCHAR(100), val INT)");
            s.execute("INSERT INTO single_delete_test VALUES (1, 'Alice', 100)");
            s.execute("INSERT INTO single_delete_test VALUES (2, 'Bob', 200)");
            s.execute("INSERT INTO single_delete_test VALUES (3, 'Charlie', 300)");
        }
        System.out.println("[PREP] 3 rows ready.");

        TableEnvironment bEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());

        String ddl =
                String.format(
                        "CREATE TABLE t ("
                                + "  id INT, name STRING, val INT,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'gaussdb',"
                                + "  'url' = '%s',"
                                + "  'table-name' = 'single_delete_test',"
                                + "  'username' = '%s',"
                                + "  'password' = '%s',"
                                + "  'sink.buffer-flush.max-rows' = '1',"
                                + "  'sink.buffer-flush.interval' = '0'"
                                + ")",
                        URL, USER, PASS);
        bEnv.executeSql(ddl);

        System.out.println("[DELETE] DELETE FROM t WHERE id = 2...");
        bEnv.executeSql("DELETE FROM t WHERE id = 2").await();
        System.out.println("[DELETE] Done.");

        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM single_delete_test ORDER BY id")) {

            System.out.println("\n=== Result ===");
            int count = 0;
            java.util.List<String> names = new java.util.ArrayList<>();
            while (rs.next()) {
                count++;
                names.add(rs.getString("name"));
                System.out.println(
                        String.format(
                                "  id=%d  name=%s  val=%d",
                                rs.getInt("id"), rs.getString("name"), rs.getInt("val")));
            }
            if (count == 2 && !names.contains("Bob")) {
                System.out.println("\n✅ Single-table batch DELETE WORKS! (" + count + " rows)");
            } else {
                System.out.println("\n❌ FAIL: " + count + " rows, names=" + names);
                throw new AssertionError("FAIL: " + names);
            }
        }

        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS single_delete_test");
        }
    }
}
