/**
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

package com.github.dariobalinzo;

import com.github.dariobalinzo.task.ElasticSourceTask;
import com.github.dariobalinzo.task.ElasticSourceTaskConfig;
import com.github.dariobalinzo.utils.ElasticSearchDataProvider;
import com.github.dariobalinzo.utils.Utils;
import com.github.dariobalinzo.utils.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ElasticSourceConnector is a Kafka Connect Connector implementation that watches an ElasticSearch index and
 * generates tasks to ingest its content.
 */
public class ElasticSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceConnector.class);

    private Map<String, String> configProperties;
    private ElasticSourceConnectorConfig config;

    /* This connection is currently used only for initial test and indices lookup
     * TODO: we'd better implement a indices monitoring thread, to detect new indices,
     * otherwise we might consider the possibility to close this connection after startup
     * and reopening it when re-assigning tasks (e.g., on rebalancing)
     */
    private ElasticSearchDataProvider elasticConnectionProvider;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting ElasticSearch Source Connector");

        try {
            configProperties = props;
            config = new ElasticSourceConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start ElasticSourceConnector due to configuration "
                    + "error", e);
        }

        elasticConnectionProvider = Utils.initElasticConnectionProvider(config);
        if (!elasticConnectionProvider.testConnection()) {
            throw new ConfigException("Cannot connect to ElasticSearch");
        }

        logger.debug("ElasticSearch");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        Response resp;
        try {
            // FIXME: wrap this HTTP call in a safer place, with retries and versioned endpoint
            resp = elasticConnectionProvider.getClient()
                    .getLowLevelClient()
                    .performRequest("GET", "_cat/indices");
        } catch (IOException e) {
            logger.error("Cannot get index names from ElasticSearch");
            throw new RuntimeException(e);
        }

        List<String> currentIndexes = Utils.parseAndSelectIndices(resp,
                config.getString(ElasticSourceConnectorConfig.INDEX_PREFIX_CONFIG)
        );

        int numGroups = Math.min(currentIndexes.size(), maxTasks);
        List<List<String>> tablesGrouped = ConnectorUtils.groupPartitions(currentIndexes, numGroups);

        List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
        for (List<String> taskIndices : tablesGrouped) {
            Map<String, String> taskProps = new HashMap<>(configProperties);
            taskProps.put(ElasticSourceTaskConfig.INDICES_CONFIG,
                    String.join(",",taskIndices));
            taskConfigs.add(taskProps);
        }

        logger.info("Task configs: {}, indexes: {}", taskConfigs, currentIndexes);
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Closing ElasticSearch connection");
        elasticConnectionProvider.closeQuietly();
        // TODO: in case we implemented an indices monitoring thread, stop it here
        logger.info("ElasticSearch connection closed");
    }

    @Override
    public ConfigDef config() {
        return ElasticSourceConnectorConfig.CONFIG_DEF;
    }
}
