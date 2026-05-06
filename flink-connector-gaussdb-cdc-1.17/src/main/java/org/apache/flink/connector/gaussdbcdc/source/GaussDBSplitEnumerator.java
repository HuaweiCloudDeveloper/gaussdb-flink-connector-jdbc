/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.gaussdbcdc.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Split enumerator for GaussDB CDC source.
 *
 * <p>Responsible for:
 *
 * <ul>
 *   <li>Creating snapshot splits for initial data
 *   <li>Creating stream split for CDC
 * </ul>
 */
@Internal
public class GaussDBSplitEnumerator implements SplitEnumerator<GaussDBSplit, GaussDBCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBSplitEnumerator.class);

    private final SplitEnumeratorContext<GaussDBSplit> context;
    private final String hostname;
    private final int port;
    private final String database;
    private final String schema;
    private final String tableName;
    private final String username;
    private final String password;
    private final boolean snapshotMode;
    private final int chunkSize;
    private final int connectTimeoutMs;
    private final String sslMode;

    private List<GaussDBSplit> snapshotSplits;
    private GaussDBSplit streamSplit;
    private boolean snapshotCompleted = false;
    private int nextSplitIndex = 0;

    // Constructor for new enumerator
    public GaussDBSplitEnumerator(
            SplitEnumeratorContext<GaussDBSplit> context,
            String hostname,
            int port,
            String database,
            String schema,
            String tableName,
            String username,
            String password,
            boolean snapshotMode,
            int chunkSize,
            int connectTimeoutMs,
            String sslMode) {
        this.context = context;
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.schema = schema;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
        this.snapshotMode = snapshotMode;
        this.chunkSize = chunkSize;
        this.connectTimeoutMs = connectTimeoutMs;
        this.sslMode = sslMode;
        this.snapshotSplits = new ArrayList<>();
    }

    // Constructor for restored enumerator
    public GaussDBSplitEnumerator(
            SplitEnumeratorContext<GaussDBSplit> context,
            GaussDBCheckpoint checkpoint,
            String hostname,
            int port,
            String database,
            String schema,
            String tableName,
            String username,
            String password,
            boolean snapshotMode,
            int chunkSize,
            int connectTimeoutMs,
            String sslMode) {
        this(
                context,
                hostname,
                port,
                database,
                schema,
                tableName,
                username,
                password,
                snapshotMode,
                chunkSize,
                connectTimeoutMs,
                sslMode);

        if (checkpoint != null) {
            this.snapshotSplits = checkpoint.getUnassignedSplits();
            this.snapshotCompleted = checkpoint.isSnapshotCompleted();
            LOG.info("Restored enumerator with {} unassigned splits", snapshotSplits.size());
        }
    }

    @Override
    public void start() {
        LOG.info(
                "Starting GaussDBSplitEnumerator for table {}.{} (snapshotMode={})",
                schema,
                tableName,
                snapshotMode);

        if (snapshotMode && !snapshotCompleted) {
            try {
                createSnapshotSplits();
            } catch (Exception e) {
                LOG.error("Failed to create snapshot splits", e);
                throw new RuntimeException("Failed to create snapshot splits", e);
            }
        }

        // Create stream split for CDC
        this.streamSplit = new GaussDBSplit("stream-split", tableName, "flink_cdc_slot", null);
    }

    private void createSnapshotSplits() throws SQLException {
        String url =
                String.format(
                        "jdbc:gaussdb://%s:%d/%s?compatibleMode=mysql&sslmode=%s",
                        hostname, port, database, sslMode);

        try (Connection conn = DriverManager.getConnection(url, username, password);
                Statement stmt = conn.createStatement()) {

            // Get min and max id
            String sql =
                    String.format(
                            "SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as cnt FROM %s.%s",
                            schema, tableName);

            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    long minId = rs.getLong("min_id");
                    long maxId = rs.getLong("max_id");
                    long count = rs.getLong("cnt");

                    LOG.info(
                            "Table {}.{} has {} rows, id range: {} to {}",
                            schema,
                            tableName,
                            count,
                            minId,
                            maxId);

                    if (count == 0) {
                        snapshotCompleted = true;
                        return;
                    }

                    // Create chunks
                    long currentId = minId;
                    int splitIndex = 0;
                    while (currentId <= maxId) {
                        long endId = Math.min(currentId + chunkSize - 1, maxId);
                        GaussDBSplit split =
                                new GaussDBSplit(
                                        "snapshot-" + splitIndex, tableName, currentId, endId);
                        snapshotSplits.add(split);
                        LOG.debug(
                                "Created snapshot split: {} (id {} to {})",
                                split.splitId(),
                                currentId,
                                endId);
                        currentId = endId + 1;
                        splitIndex++;
                    }

                    LOG.info("Created {} snapshot splits", snapshotSplits.size());
                }
            }
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // Assign next available split
        if (nextSplitIndex < snapshotSplits.size()) {
            GaussDBSplit split = snapshotSplits.get(nextSplitIndex++);
            context.assignSplit(split, subtaskId);
            LOG.info("Assigned snapshot split {} to subtask {}", split.splitId(), subtaskId);
        } else if (!snapshotCompleted) {
            // All snapshot splits assigned, now assign stream split
            snapshotCompleted = true;
            context.assignSplit(streamSplit, subtaskId);
            LOG.info("Assigned stream split to subtask {} (snapshot completed)", subtaskId);
        } else {
            // No more splits
            context.signalNoMoreSplits(subtaskId);
            LOG.info("Signaled no more splits to subtask {}", subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<GaussDBSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        // Add splits back to the beginning of the list
        for (int i = splits.size() - 1; i >= 0; i--) {
            snapshotSplits.add(0, splits.get(i));
        }
        nextSplitIndex = 0;
    }

    @Override
    public GaussDBCheckpoint snapshotState(long checkpointId) throws Exception {
        List<GaussDBSplit> unassigned = new ArrayList<>();
        for (int i = nextSplitIndex; i < snapshotSplits.size(); i++) {
            unassigned.add(snapshotSplits.get(i));
        }

        return new GaussDBCheckpoint(
                new ArrayList<>(snapshotSplits.subList(0, nextSplitIndex)),
                unassigned,
                null, // lastLsn - would be populated in real implementation
                snapshotCompleted);
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing GaussDBSplitEnumerator");
    }

    @Override
    public void addReader(int subtaskId) {
        // Reader added, no special handling needed for this implementation
        LOG.debug("Added reader for subtask {}", subtaskId);
    }
}
