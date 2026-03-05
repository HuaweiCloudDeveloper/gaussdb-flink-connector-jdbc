package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.jdbc.core.table.JdbcConnectorOptions;

/**
 * Gaussdb connector options.
 */
public class GaussdbExtendConnectorOptions extends JdbcConnectorOptions {

    public static final ConfigOption<Boolean> SINK_IGNORE_NULL_WHEN_UPDATE =
            ConfigOptions.key("sink.ignore-null-when-update")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to ignore null values when updating records. If true, null values will not be updated to the database.");
}
