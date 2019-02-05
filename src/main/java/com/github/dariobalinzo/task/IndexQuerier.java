package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * IndexQuerier executes queries against a specific index. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class IndexQuerier implements Comparable<IndexQuerier> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final ElasticsearchDAO elasticsearchDAO;
    protected final String indexName;
    protected final String targetTopic;

    // Mutable state
    protected long lastUpdate;

    protected IndexQuerier(
            ElasticsearchDAO elasticsearchDAO,
            String indexName,
            String topicPrefix
    ) {
        this.elasticsearchDAO = elasticsearchDAO;
        this.indexName = indexName;
        this.targetTopic = String.join(topicPrefix, "_", indexName);
        this.lastUpdate = 0;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public abstract boolean isScrolling();

    protected abstract void closeScrollQuietly();

    public void reset(long now) {
        closeScrollQuietly();
        lastUpdate = now;
    }

    public abstract List<SourceRecord> getRecords(int batchMaxRows);

    @Override
    public int compareTo(IndexQuerier other) {
        if (this.lastUpdate < other.lastUpdate) {
            return -1;
        } else {
            return 1;
        }
    }

}
