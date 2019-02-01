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

package com.github.dariobalinzo.elasticsearch;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.utils.Utils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.common.protocol.types.Field;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticsearchDAO {

    public final static Logger logger = LoggerFactory.getLogger(ElasticsearchDAO.class);

    public static final TimeValue SCROLL_KEEP_ALIVE = TimeValue.timeValueMinutes(1);

    private final RestHighLevelClient client;
    private final int maxConnectionAttempts;
    private final long connectionRetryBackoff;

    /**
     * Basic constructor with no authentication and a single Elasticsearch host
     *
     * @param host
     * @param port
     * @param maxConnectionAttempts
     * @param connectionRetryBackoff
     */
    public ElasticsearchDAO(
            String host,
            int port,
            int maxConnectionAttempts,
            long connectionRetryBackoff
    ) {
        logger.debug("Connecting to ElasticSearch (no authentication)");

        //TODO add configuration for https also, and many nodes instead of only one
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port)));

        this.maxConnectionAttempts = maxConnectionAttempts;
        this.connectionRetryBackoff = connectionRetryBackoff;
    }

    /**
     * Constructor with authentication and a single Elasticsearch host
     *
     * @param host
     * @param port
     * @param user
     * @param pwd
     * @param maxConnectionAttempts
     * @param connectionRetryBackoff
     */
    public ElasticsearchDAO(
            String host,
            int port,
            String user,
            String pwd,
            int maxConnectionAttempts,
            long connectionRetryBackoff
    ) {
        logger.debug("Connecting to ElasticSearch (with authentication)");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pwd));

        //TODO add configuration for https also, and many nodes instead of only one
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port)).setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                )
        );

        this.maxConnectionAttempts = maxConnectionAttempts;
        this.connectionRetryBackoff = connectionRetryBackoff;
    }

    /**
     * Test if the connection is valid, with retrying logic
     *
     * @return
     */
    public boolean testConnection() {
        try {
            return retryOnFailure(() -> {
                client.ping();
                return true;
            });
        } catch (Exception e) {
            logger.error("Connection test failed", e);
            return false;
        }
    }

    /**
     * Shutdown Elasticsearch client
     */
    public void closeQuietly() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            logger.error("Error while closing ElasticsearchDAO", e);
        }
    }

    /**
     * Get all indices found on Elasticsearch, with retrying logic
     *
     * @return
     * @throws IOException
     */
    public List<String> getIndices() throws IOException {
        return retryOnFailure(() -> {
            final List<String> result = new LinkedList<>();

            final Response response = client.getLowLevelClient()
                    .performRequest("GET", "_cat/indices");

            final InputStream rawResponse = response.getEntity().getContent();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(rawResponse));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String index = line.split("\\s+")[2];
                result.add(index);
            }

            return result;
        });
    }

    /**
     * Get all indices that matches a certain prefix, with retrying logic
     *
     * @param prefix
     * @return
     * @throws IOException
     */
    public List<String> getIndicesMatching(String prefix) throws IOException {
        return getIndices()
                .stream()
                .filter(index -> index.startsWith(prefix))
                .collect(Collectors.toList());
    }






















    public SearchResponse performSearchRequest(SearchRequest searchRequest) throws IOException {
        return retryOnFailure(() -> {
            SearchResponse response = client.search(searchRequest);
            if (response == null) {
                throw new RuntimeException("Null response received from ElasticSearch client");
            }

            SearchHits hits = response.getHits();
            int totalShards = response.getTotalShards();
            int successfulShards = response.getSuccessfulShards();
            logger.info("Total shard: {} - Successful: {} - Hits: {}", totalShards, successfulShards, hits.totalHits);

            int failedShards = response.getFailedShards();
            if (failedShards > 0) {
                for (ShardSearchFailure failure : response.getShardFailures()) {
                    logger.error("Failed shards information: {}", failure);
                }
                // TODO: better handle shard failures
                throw new RuntimeException("failed shard in search");
            }

            return response;
        });
    }


    /**
     * Wrapper method that encapsulate retrying logic
     *
     * @param action
     * @param <R>
     * @param <E>
     * @return
     * @throws E
     * @throws IOException
     */
    protected <R,E extends IOException> R retryOnFailure(ElasticsearhRetriableAction<R,E> action) throws E, IOException {
        for (int i = 0; i < maxConnectionAttempts; ++i) {
            try {
                return action.apply();
            } catch (IOException e) {
                logger.warn("Connection problems with ElasticSearch (attempt: {}) - Trying again in {} ms...", i, connectionRetryBackoff, e);
            }

            try {
                Thread.sleep(connectionRetryBackoff);
            } catch (InterruptedException ignored) {
                //we should not be interrupted here..
            }
        }

        throw new IOException(String.format("Failed %s connection attempts (every %s ms) - limit reached", maxConnectionAttempts, connectionRetryBackoff));
    }

    /**
     * Whatever action we might want to retry for a certain number of times
     *
     * @param <R>
     * @param <E>
     */
    protected interface ElasticsearhRetriableAction<R,E extends IOException> {
        public R apply() throws E;
    }


    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 9200;
        int maxConnectionAttempts = 5;
        long connectionRetryBackoff = 1000;
        String prefix = "jago_prod";

        final ElasticsearchDAO dao = new ElasticsearchDAO(host, port, maxConnectionAttempts, connectionRetryBackoff);

        logger.info("Connection is working: {}", dao.testConnection());

        logger.info("All indices available: {}", dao.getIndices());

        logger.info("All indices matching '{}': {}", prefix, dao.getIndicesMatching(prefix));

        logger.info("Closing client...");
        dao.closeQuietly();
        logger.info("Client closed");


    }

}
