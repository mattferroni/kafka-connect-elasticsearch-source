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

package com.github.dariobalinzo;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Collections;
import java.util.Map;

public class ElasticSourceConnectorConfig extends AbstractConfig {

    //TODO add the possibility to specify multiple hosts
    public final static String ES_HOST_CONF = "es.host";
    private final static String ES_HOST_DOC = "Elasticsearch host.";
    private final static String ES_HOST_DISPLAY = "Elasticsearch host";

    public final static String ES_PORT_CONF = "es.port";
    private final static String ES_PORT_DOC = "Elasticsearch port.";
    private final static String ES_PORT_DISPLAY = "Elasticsearch port";

    public final static String ES_USER_CONF = "es.user";
    private final static String ES_USER_DOC = "Elasticsearch username.";
    private final static String ES_USER_DISPLAY = "Elasticsearch username";

    public final static String ES_PWD_CONF = "es.password";
    private final static String ES_PWD_DOC = "Elasticsearch password.";
    private final static String ES_PWD_DISPLAY = "Elasticsearch password";

    public static final String CONNECTION_ATTEMPTS_CONFIG = "connection.attempts";
    private static final String CONNECTION_ATTEMPTS_DOC
            = "Maximum number of attempts to retrieve a valid Elasticsearch connection.";
    private static final String CONNECTION_ATTEMPTS_DISPLAY = "Elasticsearch connection attempts";
    private static final Integer CONNECTION_ATTEMPTS_DEFAULT = 6;

    public static final String CONNECTION_BACKOFF_CONFIG = "connection.backoff.ms";
    private static final String CONNECTION_BACKOFF_DOC
            = "Backoff time in milliseconds between connection attempts.";
    private static final String CONNECTION_BACKOFF_DISPLAY
            = "Elastic connection backoff in milliseconds";
    private static final Long CONNECTION_BACKOFF_DEFAULT = 10000l;

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
            + "each index.";
    private static final Long POLL_INTERVAL_MS_DEFAULT = 5000l;
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    private static final String BATCH_MAX_ROWS_DOC =
            "Maximum number of documents to include in a single batch when polling for new data.";
    private static final Integer BATCH_MAX_ROWS_DEFAULT = 1000;
    private static final String BATCH_MAX_ROWS_DISPLAY = "Max Documents Per Batch";

    public static final String MODE_UNSPECIFIED = "";
    public static final String MODE_BULK = "bulk";
    public static final String MODE_TIMESTAMP = "timestamp";
    public static final String MODE_INCREMENTING = "incrementing";
    public static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

    public static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC = "The mode for updating a table each time it is polled. " +
            "Now supporting only " + MODE_INCREMENTING + " mode: use a strictly incrementing column " +
            "on each table to detect only new rows. Note that this will not detect modifications or " +
            "deletions of existing rows";
    private static final String MODE_DISPLAY = "Index loading mode";

    public static final String INCREMENTING_FIELD_NAME_CONFIG = "incrementing.field.name";
    private static final String INCREMENTING_FIELD_NAME_DOC =
            "The name of the strictly incrementing field to use to detect new records.";
    private static final String INCREMENTING_FIELD_NAME_DEFAULT = "timestamp";
    private static final String INCREMENTING_FIELD_NAME_DISPLAY = "Incrementing Field Name";

    public static final String INDEX_PREFIX_CONFIG = "index.prefix";
    private static final String INDEX_PREFIX_DOC = "List of indices to include in copying.";
    private static final String INDEX_PREFIX_DISPLAY = "Indices prefix Whitelist";

    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC =
            "Prefix to prepend to index names to generate the name of the Kafka topic to publish data.";
    private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";

    private static final String DATABASE_GROUP = "Elasticsearch";
    private static final String MODE_GROUP = "Mode";
    private static final String CONNECTOR_GROUP = "Connector";


    public static final ConfigDef CONFIG_DEF = baseConfigDef();


    protected static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addDatabaseOptions(config);
        addModeOptions(config);
        addConnectorOptions(config);
        return config;
    }

    private static void addDatabaseOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                ES_HOST_CONF,
                Type.STRING,
                Importance.HIGH,
                ES_HOST_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_HOST_DISPLAY,
                Collections.singletonList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_PORT_CONF,
                Type.INT,
                Importance.HIGH,
                ES_PORT_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_PORT_DISPLAY,
                Collections.singletonList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_USER_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                ES_USER_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_USER_DISPLAY
        ).define(
                ES_PWD_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                ES_PWD_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                ES_PWD_DISPLAY
        ).define(
                CONNECTION_ATTEMPTS_CONFIG,
                Type.INT,
                CONNECTION_ATTEMPTS_DEFAULT,
                Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
        ).define(
                CONNECTION_BACKOFF_CONFIG,
                Type.LONG,
                CONNECTION_BACKOFF_DEFAULT,
                Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
        ).define(
                INDEX_PREFIX_CONFIG,
                Type.STRING,
                Importance.MEDIUM,
                INDEX_PREFIX_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                INDEX_PREFIX_DISPLAY
        );
    }

    private static void addModeOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                MODE_CONFIG,
                Type.STRING,
                MODE_UNSPECIFIED,
                ConfigDef.ValidString.in(
                        MODE_UNSPECIFIED,
                        MODE_BULK,
                        MODE_TIMESTAMP,
                        MODE_INCREMENTING,
                        MODE_TIMESTAMP_INCREMENTING
                ),
                Importance.HIGH,
                MODE_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                MODE_DISPLAY,
                Collections.singletonList(
                        INCREMENTING_FIELD_NAME_CONFIG
                )
        ).define(
                INCREMENTING_FIELD_NAME_CONFIG,
                Type.STRING,
                INCREMENTING_FIELD_NAME_DEFAULT,
                Importance.MEDIUM,
                INCREMENTING_FIELD_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                INCREMENTING_FIELD_NAME_DISPLAY
        );
    }

    private static void addConnectorOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                POLL_INTERVAL_MS_CONFIG,
                Type.LONG,
                POLL_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY
        ).define(
                BATCH_MAX_ROWS_CONFIG,
                Type.INT,
                BATCH_MAX_ROWS_DEFAULT,
                Importance.LOW,
                BATCH_MAX_ROWS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                BATCH_MAX_ROWS_DISPLAY
        ).define(
                TOPIC_PREFIX_CONFIG,
                Type.STRING,
                Importance.HIGH,
                TOPIC_PREFIX_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TOPIC_PREFIX_DISPLAY
        );
    }

    public ElasticSourceConnectorConfig(Map<String, String> properties) {
        super(CONFIG_DEF, properties);
    }

    protected ElasticSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
        super(subclassConfigDef, props);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
