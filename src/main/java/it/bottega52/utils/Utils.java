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

package it.bottega52.utils;

import it.bottega52.ElasticSourceConnectorConfig;
import it.bottega52.elasticsearch.ElasticsearchDAO;
import it.bottega52.task.ElasticSourceTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class Utils {

    public static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static ElasticsearchDAO initElasticsearchDAO(
            final ElasticSourceConnectorConfig config
    ) {
        final String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        final int esPort = config.getInt(ElasticSourceConnectorConfig.ES_PORT_CONF);

        final String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        final String esPwd = config.getString(ElasticSourceConnectorConfig.ES_PWD_CONF);

        final int maxConnectionAttempts = config.getInt(ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long connectionRetryBackoff = config.getLong(ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);

        final ElasticsearchDAO elasticConnectionProvider;
        if (esUser == null || esUser.isEmpty()) {
            elasticConnectionProvider = new ElasticsearchDAO(
                    esHost,
                    esPort,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );
        } else {
            elasticConnectionProvider = new ElasticsearchDAO(
                    esHost,
                    esPort,
                    esUser,
                    esPwd,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );
        }
        return elasticConnectionProvider;
    }

    public static Map<String, String> generateKeyForOffsetsTopic(String indexName) {
        return Collections.singletonMap(
                ElasticSourceTaskConfig.KEY_FOR_OFFSETS_KEY,
                indexName
        );
    }

    public static Map<String, String> generateValueForOffsetsTopic(String incrementingFieldLastValue) {
        return Collections.singletonMap(
                ElasticSourceTaskConfig.KEY_FOR_OFFSETS_VALUE,
                incrementingFieldLastValue
        );
    }

    // Not all elastic names are valid avro name
    public static String sanitizeName(String fieldName) {
        return fieldName == null ? null : fieldName.replaceAll("[^a-zA-Z0-9]", "");
    }

    public static String sanitizeName(String containerName, String fieldName) {
        return fieldName == null ? containerName : containerName+fieldName.replaceAll("[^a-zA-Z0-9]", "");
    }
}
