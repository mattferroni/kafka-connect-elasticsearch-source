/*
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import com.github.dariobalinzo.utils.Utils;
import com.github.dariobalinzo.utils.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * ElasticSourceTask is a Kafka Connect SourceTask implementation that reads from ElasticSearch
 * indices and generates Kafka Connect records.
 */
public class ElasticSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceTask.class);

    // TODO: ok here?
    private static final String OFFSET_NAME_FIELD = "OFFSET_NAME_FIELD";
    private static final String OFFSET_KEY_KEY = "OFFSET_KEY_KEY";
    private static final String OFFSET_KEY_VALUE = "OFFSET_KEY_VALUE";

    private final Time time;
    private ElasticSourceTaskConfig config;
    private ElasticsearchDAO elasticsearchDAO;

    private PriorityQueue<IndexQuerier> indicesQueue = new PriorityQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ElasticSourceTask() {
        this.time = new SystemTime();
    }

    public ElasticSourceTask(Time time) {
        this.time = time;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        logger.info("Starting ElasticSourceTask");

        try {
            config = new ElasticSourceTaskConfig(properties);
        } catch (ConfigException e) {
            throw new ConfigException("Couldn't start ElasticSourceTask due to configuration error", e);
        }

        elasticsearchDAO = Utils.initElasticsearchDAO(config);
        if (!elasticsearchDAO.testConnection()) {
            throw new ConnectException("Cannot connect to ElasticSearch");
        }

        logger.debug("Connected to ElasticSearch");

        /* TODO: add here multiple mode configs, e.g.: (inspiration from JDBC connector)
         * - bulk - perform a bulk load of the entire table each time it is polled
         * - incrementing - use a strictly incrementing column on each table to detect
         *      only new rows. Note that this will not detect modifications or deletions
         *      of existing rows
         * - timestamp - use a timestamp (or timestamp-like) column to detect new and
         *      modified rows. This assumes the column is updated with each write, and
         *      that values are monotonically incrementing, but not necessarily unique
         * - timestamp+incrementing - use two columns, a timestamp column that detects
         *      new and modified rows and a strictly incrementing column which provides a
         *      globally unique ID for updates so each row can be assigned a unique stream
         *      offset
        */
        String mode = config.getString(ElasticSourceTaskConfig.MODE_CONFIG);
        if (!mode.equals(ElasticSourceTaskConfig.MODE_INCREMENTING)) {
            throw new ConfigException("Current version supports only mode: " + ElasticSourceTaskConfig.MODE_INCREMENTING);
        }

        final String topicPrefix = config.getString(ElasticSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
        final String incrementingField = config.getString(ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG);

        final List<String> indices = Arrays.asList(config.getString(ElasticSourceTaskConfig.INDICES_CONFIG).split(","));
        if (indices.isEmpty()) {
            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
                    + "least one index assigned to it");
        }

        List<Map<String, String>> partitions = indices.stream()
                .map(indexName -> Collections.singletonMap(OFFSET_NAME_FIELD, indexName))
                .collect(Collectors.toList());
        Map<Map<String, String>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(partitions);
        logger.trace("The partition offsets are {}", offsets);

        for (String index : indices) {
            // TODO: implement here multi-protocol support for offsets, if needed
            // The partition map varies by offset protocol. Since we don't know which protocol each
            // table's offsets are keyed by, we need to use the different possible partitions
            // (newest protocol version first) to find the actual offsets for each table.
            Map<String, Object> offset = null;
            if (offsets != null) {
                // TODO: delirio here!
                // for (Map<String, String> toCheckPartition : tablePartitionsToCheck) {
                    String toCheckPartition = index;
                    offset = offsets.get(toCheckPartition);
                    if (offset != null) {
                        logger.info("Found offset {} for partition {}", offsets, toCheckPartition);
                        break;
                    }
                // }
            }

            indicesQueue.add(new IncrementingIndexQuerier(
                    index,
                    topicPrefix,
                    incrementingField,
                    offset,
                    elasticConnectionProvider
            ));
        }

        running.set(true);
        logger.info("Started ElasticSourceTask");
    }

    /*
     * This method is synchronized as SourceTasks are given a dedicated thread which they can block
     * indefinitely, so they need to be stopped with a call from a different thread in the Worker
     */
    @Override
    public synchronized void stop() {
        logger.info("Stopping ElasticSourceTask");
        running.set(false);
        // All resources are closed at the end of 'poll()' when no longer running or
        // if there is an error
    }

    @Override
    public List<SourceRecord> poll() {
        logger.trace("ElasticSourceTask polling for new data");

        while (running.get()) {
            final IndexQuerier querier = indicesQueue.peek();

            if (!querier.isScrolling()) {
                // Wait for next update time
                final long nextUpdate = querier.getLastUpdate()
                        + config.getInt(ElasticSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
                final long untilNext = nextUpdate - time.milliseconds();
                if (untilNext > 0) {
                    logger.trace("Waiting {} ms to poll {} next", untilNext, querier.toString());
                    time.sleep(untilNext);
                    continue; // Re-check stop flag before continuing
                } else {
                    logger.trace("{} can proceed with a new scroll query", querier.toString());
                }
            }

            try {
                logger.debug("Checking for next block of results from {}", querier.toString());

                final int batchMaxRows = config.getInt(ElasticSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
                final List<SourceRecord> sourceRecords = querier.getRecords(elasticsearchDAO, batchMaxRows);

                /* TODO:
                if it was scrolling
                    try to continue
                    in case scroll was closed, invalidte it and try again later
                else
                    start a new scroll

                */

                if (sourceRecords.size() < batchMaxRows) {
                    // If we finished processing the results from the current query, we can reset and send
                    // the querier to the tail of the queue
                    resetAndRequeueHead(querier);
                }

                if (sourceRecords.isEmpty()) {
                    logger.trace("No updates for {}", querier.toString());
                    continue;
                }

                logger.debug("Returning {} records for {}", sourceRecords.size(), querier.toString());
                return sourceRecords;
            } catch (RuntimeException e) {
                logger.error("Failed to run query for index: {} - Continue with the next index...", querier.toString(), e);
                resetAndRequeueHead(querier);
                return Collections.emptyList();
            } catch (Throwable t) {
                resetAndRequeueHead(querier);
                // This task has failed, so close any resources (may be reopened if needed) before throwing
                closeResources();
                throw t;
            }
        }

        // Only in case of shutdown
        final IndexQuerier querier = indicesQueue.peek();
        if (querier != null) {
            resetAndRequeueHead(querier);
        }
        closeResources();
        return Collections.emptyList();
    }

    private void resetAndRequeueHead(IndexQuerier expectedHead) {
        logger.debug("Resetting querier {}", expectedHead.toString());
        IndexQuerier removedQuerier = indicesQueue.poll();
        assert removedQuerier == expectedHead;
        expectedHead.reset(time.milliseconds(), elasticsearchDAO);
        indicesQueue.add(expectedHead);
    }

    protected void closeResources() {
        logger.info("Closing resources for ElasticSourceTask");
        try {
            if (elasticsearchDAO != null) {
                elasticsearchDAO.closeQuietly();
            }
        } catch (Throwable t) {
            logger.error("Error while closing the connections", t);
        } finally {
            elasticsearchDAO = null;
        }
    }

}
