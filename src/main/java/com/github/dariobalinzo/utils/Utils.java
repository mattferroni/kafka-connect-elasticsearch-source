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

package com.github.dariobalinzo.utils;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Utils {

    public static final Logger logger = LoggerFactory.getLogger(Utils.class);

    // TODO: fix these to find a better naming
    public static final String FILENAME_FIELD = "elasticsource-filename-field";
    public static final String POSITION_FIELD = "elasticsource-position-field";

    public static ElasticsearchDAO initElasticConnectionProvider(final ElasticSourceConnectorConfig config) {
        final String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        final int esPort = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.ES_PORT_CONF));

        final String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        final String esPwd = config.getString(ElasticSourceConnectorConfig.ES_PWD_CONF);

        final int maxConnectionAttempts = Integer.parseInt(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        ));
        final long connectionRetryBackoff = Long.parseLong(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        ));

        final long batchMaxRows = Long.parseLong(config.getString(
                ElasticSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG
        ));

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

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }



    //not all elastic names are valid avro name
    public static String filterAvroName(String elasticName) {
        return elasticName == null ? null:elasticName.replaceAll("[^a-zA-Z0-9]", "");
    }

    public static String filterAvroName(String prefix, String elasticName) {
        return elasticName == null ? prefix:prefix+elasticName.replaceAll("[^a-zA-Z0-9]", "");
    }
}
