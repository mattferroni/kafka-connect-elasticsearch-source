package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;

/**
 * IndexQuerier executes queries against a specific index. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class IndexQuerier implements Comparable<IndexQuerier> {

    protected final String topicPrefix;
    protected final String indexName;
    protected final ElasticsearchDAO elasticsearchDAO;

    // Mutable state
    protected long lastUpdate;
    protected String currentScrollId;

    protected IndexQuerier(
            ElasticsearchDAO elasticsearchDAO,
            String indexName,
            String topicPrefix
    ) {
        this.elasticsearchDAO = elasticsearchDAO;
        this.indexName = indexName;
        this.topicPrefix = topicPrefix;
        this.lastUpdate = 0;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public boolean isScrolling() {
        return currentScrollId !=null;
    }

    public void reset(long now) {
        closeScrollQuietly();
        lastUpdate = now;
    }

    public abstract List<SourceRecord> getRecords(int batchMaxRows);

    private void closeScrollQuietly() {
        if (currentScrollId != null) {
            try {
                elasticsearchDAO.closeScrollQuietly(currentScrollId);
            } catch (IOException ignored) {
                // intentionally ignored
            }
        }
        currentScrollId = null;
    }

    @Override
    public int compareTo(IndexQuerier other) {
        if (this.lastUpdate < other.lastUpdate) {
            return -1;
        } else {
            return 1;
        }
    }

}
