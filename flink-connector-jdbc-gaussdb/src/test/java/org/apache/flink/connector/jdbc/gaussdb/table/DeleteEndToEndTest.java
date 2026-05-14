package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/** End-to-end test to verify DELETE statement support against cloud GaussDB. */
public class DeleteEndToEndTest extends AbstractTestBase {

    private static final String URL = "jdbc:gaussdb://1.92.120.69:8000/postgres";
    private static final String USER = "root";
    private static final String PASS = "GaussDB123";

    @Test
    public void testDeleteEndToEnd() throws Exception {
        // Step 1: Prepare table and insert data via JDBC
        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement()) {
            s.execute(
                    "CREATE TABLE IF NOT EXISTS delete_e2e_test (id INT PRIMARY KEY, name VARCHAR(100), val INT)");
            s.execute("DELETE FROM delete_e2e_test");
            s.execute("INSERT INTO delete_e2e_test VALUES (1, 'Alice', 100)");
            s.execute("INSERT INTO delete_e2e_test VALUES (2, 'Bob', 200)");
            s.execute("INSERT INTO delete_e2e_test VALUES (3, 'Charlie', 300)");
        }
        System.out.println("[PREP] Table ready: 3 rows inserted.");

        // Step 2: Run Flink job that produces DELETE for id=2 (Bob)
        // We use a changelog source with INSERT for 3 rows + DELETE for id=2
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        // Register changelog data: INSERT(1), INSERT(2), INSERT(3), DELETE(2)
        // This leaves only rows 1 and 3 in the sink
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                // INSERT 3 rows
                                Row.ofKind(RowKind.INSERT, 1, "Alice", 100),
                                Row.ofKind(RowKind.INSERT, 2, "Bob", 200),
                                Row.ofKind(RowKind.INSERT, 3, "Charlie", 300),
                                // DELETE Bob
                                Row.ofKind(RowKind.DELETE, 2, "Bob", 200)));

        String sourceDdl =
                String.format(
                        "CREATE TABLE changelog_src (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  val INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'changelog-mode' = 'I,D'\n"
                                + ")",
                        dataId);
        tEnv.executeSql(sourceDdl);

        String sinkDdl =
                String.format(
                        "CREATE TABLE delete_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  val INT,\n"
                                + "  PRIMARY KEY (id) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'gaussdb',\n"
                                + "  'url' = '%s',\n"
                                + "  'table-name' = 'delete_e2e_test',\n"
                                + "  'username' = '%s',\n"
                                + "  'password' = '%s',\n"
                                + "  'sink.buffer-flush.max-rows' = '1',\n"
                                + "  'sink.buffer-flush.interval' = '0'\n"
                                + ")",
                        URL, USER, PASS);
        tEnv.executeSql(sinkDdl);

        System.out.println(
                "[FLINK] Executing INSERT INTO delete_sink SELECT * FROM changelog_src...");
        tEnv.executeSql("INSERT INTO delete_sink SELECT * FROM changelog_src").await();
        System.out.println("[FLINK] Job completed.");

        // Step 3: Verify results via JDBC
        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM delete_e2e_test ORDER BY id")) {

            System.out.println("\n=== Final table contents ===");
            int count = 0;
            List<String> names = new java.util.ArrayList<>();
            while (rs.next()) {
                count++;
                String name = rs.getString("name");
                names.add(name);
                System.out.println(
                        String.format(
                                "  id=%d  name=%s  val=%d",
                                rs.getInt("id"), name, rs.getInt("val")));
            }
            System.out.println("Total rows: " + count);

            if (count == 2
                    && !names.contains("Bob")
                    && names.contains("Alice")
                    && names.contains("Charlie")) {
                System.out.println(
                        "\n✅ DELETE WORKS! Bob was correctly deleted. Only Alice and Charlie remain.");
            } else {
                System.out.println(
                        "\n❌ FAIL: Expected 2 rows (Alice, Charlie), got "
                                + count
                                + " rows: "
                                + names);
                throw new AssertionError("DELETE test failed: " + names);
            }
        }

        // Cleanup
        try (Connection c = DriverManager.getConnection(URL, USER, PASS);
                Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS delete_e2e_test");
        }
    }
}
