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

    // Mutable state
    protected long lastUpdate;
    protected String currentScrollId;

    protected IndexQuerier(
            String indexName,
            String topicPrefix
    ) {
        this.indexName = indexName;
        this.topicPrefix = topicPrefix;
        this.lastUpdate = 0;
    }

    public abstract boolean isScrolling();

    public abstract List<SourceRecord> getRecords(ElasticsearchDAO elasticsearchDAO, int batchMaxRows);

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void reset(long now, ElasticsearchDAO elasticsearchDAO) {
        closeScrollQuietly(elasticsearchDAO);
        lastUpdate = now;
    }

    private void closeScrollQuietly(ElasticsearchDAO elasticsearchDAO) {
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
