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

package it.bottega52.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;

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
        logger.debug("Connecting to Elasticsearch (no authentication)");

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
        logger.debug("Connecting to Elasticsearch (with authentication)");

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
     * Test if the connection is valid
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
     * Get all indices found on Elasticsearch
     *
     * @return
     * @throws IOException
     */
    public List<String> getIndices() throws IOException {
        return retryOnFailure(() -> {
            final List<String> result = new LinkedList<>();

            final Response response = retryOnFailure(() -> {
                return client.getLowLevelClient()
                        .performRequest("GET", "_cat/indices");
            });

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
     * Get all indices that matches a certain prefix
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

    /**
     * Get the first (oldest) value in the index for a given field; it might
     * return an empty result if no documents are present or the field do not
     * exist.
     *
     * @param index
     * @param incrementingFieldName
     * @return
     * @throws IOException
     */
    public Optional<String> getFirstValue(String index, String incrementingFieldName) throws IOException {
        final SearchRequest searchRequest = new SearchRequest(index);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
                .query(matchAllQuery())
                .sort(incrementingFieldName, SortOrder.ASC)
                .size(1);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = retryOnFailure(() -> {
            logger.debug("Get first value of field: {} on index: {} - Complete request: {}", incrementingFieldName, index, searchRequest);
            final SearchResponse response = client.search(searchRequest);
            return response;
        });

        final SearchHits hits = searchResponse.getHits();
        if (hits == null || hits.totalHits == 0 || hits.getHits() == null || hits.getHits().length == 0 || !hits.getHits()[0].getSourceAsMap().containsKey(incrementingFieldName)) {
            logger.warn("No first value found for field: {} - Complete response: {}", incrementingFieldName, searchResponse);
            return Optional.empty();
        } else {
            final String firstValue = hits.getHits()[0].getSourceAsMap().get(incrementingFieldName).toString();
            logger.debug("First value found for : {} -> {}", incrementingFieldName, firstValue);
            return Optional.of(firstValue);
        }
    }

    /**
     * Start a scroll query that ranges from the last value of
     * incrementingFieldName, expecting maximum batchMaxRows items per request
     *
     * @param index
     * @param incrementingFieldName
     * @param incrementingFieldLastValue
     * @param includeLower
     * @param batchMaxRows
     * @return
     * @throws IOException
     */
    public ElasticsearchScrollResponse startScroll(String index, String incrementingFieldName, String incrementingFieldLastValue, boolean includeLower, int batchMaxRows) throws IOException {
        final SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(SCROLL_KEEP_ALIVE);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                rangeQuery(incrementingFieldName)
                        .from(incrementingFieldLastValue, includeLower)
        )
                .sort(incrementingFieldName, SortOrder.ASC)
                .size(batchMaxRows);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = retryOnFailure(() -> {
            logger.debug("Executing request on index: {}, field name: {}, last value: {} - Complete request: {}", index, incrementingFieldName, incrementingFieldLastValue, searchRequest);
            final SearchResponse response = client.search(searchRequest);
            return response;
        });

        final ElasticsearchScrollResponse response = new ElasticsearchScrollResponse(searchResponse.getHits(), searchResponse.getScrollId());
        logger.debug("ElasticsearchScrollResponse: {}", response);
        return response;
    }

    /**
     * Continue scroll query, given its id
     *
     * @param scrollId
     * @return
     * @throws IOException
     */
    public ElasticsearchScrollResponse continueScroll(final String scrollId) throws IOException {
        final SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(SCROLL_KEEP_ALIVE);

        SearchResponse searchResponse = retryOnFailure(() -> {
            logger.debug("Continue scroll with id: {} - Complete request: {}", scrollId, scrollRequest);
            final SearchResponse response = client.searchScroll(scrollRequest);
            return response;
        });

        final ElasticsearchScrollResponse response = new ElasticsearchScrollResponse(searchResponse.getHits(), searchResponse.getScrollId());
        logger.info("ElasticsearchScrollResponse: {}", response);
        return response;
    }

    /**
     * Close an ongoing scroll
     *
     * @param scrollId
     */
    public void closeScrollQuietly(String scrollId) throws IOException {
        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);

        client.clearScrollAsync(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse clearScrollResponse) {
                boolean succeeded = clearScrollResponse != null && clearScrollResponse.isSucceeded();
                if (succeeded) {
                    logger.debug("Gracefully closed scroll request with id: {}", scrollId);
                } else {
                    logger.warn("Error closing scroll request with id: {} - Full response: {} - Continue anyway...", scrollId, clearScrollResponse);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e.getMessage().contains("response=HTTP/1.1 200 OK")) {
                    logger.info("Exception closing scroll request with id: {}, but Elastic responded correctly - Which Elasticsearch version are you targeting?", scrollId);
                } else {
                    logger.warn("Error closing scroll request with id: {} - Continue anyway...", scrollId, e);
                }
            }
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
                R response = action.apply();

                if (response instanceof SearchResponse) {
                    if (response == null) {
                        throw new IOException("Null response received from Elasticsearch client");
                    }

                    final SearchHits hits = ((SearchResponse) response).getHits();
                    assert (hits != null) : "Null hits received from Elasticsearch client";

                    final String scrollId = ((SearchResponse) response).getScrollId();
                    assert (scrollId != null) : "Null scrollId received from Elasticsearch client";

                    final int totalShards = ((SearchResponse) response).getTotalShards();
                    final int successfulShards = ((SearchResponse) response).getSuccessfulShards();
                    logger.debug("Total shard: {} - Successful: {} - Total hits: {} - Current hits: {}", totalShards, successfulShards, hits.totalHits, hits.getHits().length);

                    int failedShards = ((SearchResponse) response).getFailedShards();
                    if (failedShards > 0) {
                        for (ShardSearchFailure failure : ((SearchResponse) response).getShardFailures()) {
                            logger.error("Failed shards information: {}", failure);
                        }
                        // TODO: better handle shard failures
                        throw new IOException("Failed shard in search");
                    }
                }

                return response;
            } catch (IOException e) {
                logger.warn("Connection problems with Elasticsearch (attempt: {}) - Trying again in {} ms...", i, connectionRetryBackoff, e);
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
     * Simple tests for local debugging
     *
     * TODO: make these real tests!
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 9200;
        int maxConnectionAttempts = 5;
        long connectionRetryBackoff = 1000;
        int batchMaxRow = 1000;
        String prefix = "jago_sand";
        String fieldName = "creationDate";
        String notExistingFieldName = "sticazzi";

        final ElasticsearchDAO dao = new ElasticsearchDAO(host, port, maxConnectionAttempts, connectionRetryBackoff);

        logger.info("Connection is working: {}", dao.testConnection());

        logger.info("All indices available: {}", dao.getIndices());

        final List<String> indices = dao.getIndicesMatching(prefix);
        logger.info("All indices matching '{}': {}", prefix, indices);

        final String index = indices.get(0);

        Optional<String> firstValue = dao.getFirstValue(index, fieldName);
        logger.info("First value for field: {} -> {}", fieldName, firstValue);

        try {
            firstValue = dao.getFirstValue(index, notExistingFieldName);
        } catch (ElasticsearchStatusException e) {
            logger.info("No value found for field: {} - Exception: {}", notExistingFieldName, e);
        }

        ElasticsearchScrollResponse resp = dao.startScroll(index, fieldName, "0", true, batchMaxRow);
        logger.info("Scroll query from beginning: {}", resp);

        long expectedHits = resp.getHits().totalHits;
        long scrolledHit = resp.getHits().getHits().length;
        while(resp.getHits().getHits().length > 0) {
            resp = dao.continueScroll(resp.getScrollId());
            logger.info("Another scroll returned: {} hits", resp.getHits().getHits().length);
            scrolledHit += resp.getHits().getHits().length;
        }
        boolean success = expectedHits == scrolledHit;
        if (success) {
            logger.info("First scroll completed correctly");
        } else {
            logger.error("First scroll had total: {} but got: {} hits", expectedHits, scrolledHit);
        }

        resp = dao.startScroll(index, fieldName, "1544616732000", true, batchMaxRow);
        logger.info("Scroll query from beginning: {}", resp);

        expectedHits = resp.getHits().totalHits;
        scrolledHit = resp.getHits().getHits().length;
        while(resp.getHits().getHits().length > 0) {
            resp = dao.continueScroll(resp.getScrollId());
            logger.info("Another scroll returned: {} hits", resp.getHits().getHits().length);
            scrolledHit += resp.getHits().getHits().length;
        }
        success = expectedHits == scrolledHit;
        if (success) {
            logger.info("Second scroll completed correctly");
        } else {
            logger.error("Second scroll had total: {} but got: {} hits", expectedHits, scrolledHit);
        }

        logger.info("Closing client...");
        dao.closeQuietly();
        logger.info("Client closed");
    }

}
