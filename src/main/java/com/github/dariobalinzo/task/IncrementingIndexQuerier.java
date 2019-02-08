package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import com.github.dariobalinzo.elasticsearch.ElasticsearchScrollResponse;
import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import com.github.dariobalinzo.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;

public class IncrementingIndexQuerier extends IndexQuerier {

    private final String incrementingFieldName;

    // Mutable state
    private Optional<String> incrementingFieldFirstValue;
    private Optional<String> incrementingFieldLastValue;
    protected Optional<String> currentScrollId;

    public IncrementingIndexQuerier(
            ElasticsearchDAO elasticsearchDAO,
            String indexName,
            String topicPrefix,
            String incrementingFieldName
    ) {
        super(elasticsearchDAO, indexName, topicPrefix);
        this.incrementingFieldName = incrementingFieldName;

        this.incrementingFieldFirstValue = getFirstFieldValueFromIndex();
        this.incrementingFieldLastValue = Optional.empty();
        this.currentScrollId = Optional.empty();
    }

    public IncrementingIndexQuerier(
            ElasticsearchDAO elasticsearchDAO,
            String indexName,
            String topicPrefix,
            String incrementingFieldName,
            String incrementingFieldLastValue
    ) {
        this(elasticsearchDAO, indexName, topicPrefix, incrementingFieldName);

        assert (incrementingFieldLastValue != null) : "incrementingFieldLastValue should not be null, use the right constructor instead";
        this.incrementingFieldLastValue = Optional.of(incrementingFieldLastValue);
    }

    protected void closeScrollQuietly() {
        if (currentScrollId.isPresent()) {
            try {
                elasticsearchDAO.closeScrollQuietly(currentScrollId.get());
                logger.debug("Scroll closed correctly");
            } catch (IOException e) {
                logger.error("Error while closing scroll with id: {}", currentScrollId, e);
            } finally {
                currentScrollId = Optional.empty();
            }
        }
    }

    private Optional<String> getFirstFieldValueFromIndex() {
        try {
            logger.debug("Looking for the oldest document in the index: {}", indexName);

            Optional<String> firstValueOnIndex = elasticsearchDAO.getFirstValue(indexName, incrementingFieldName);
            if (firstValueOnIndex.isPresent()) {
                logger.info("Oldest value for field: {} on index: {} is: {}", this.incrementingFieldName, this.indexName, firstValueOnIndex.get());
            } else {
                logger.warn("Not able to find a value for field: {} on index: {}", this.incrementingFieldName, this.indexName);
            }

            return firstValueOnIndex;
        } catch (IOException e) {
            logger.error("Exception while looking for the oldest document", e);
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("IncrementingIndexQuerier{");
        sb.append("incrementingFieldName='").append(incrementingFieldName).append('\'');
        sb.append(", incrementingFieldLastValue='").append(incrementingFieldLastValue).append('\'');
        sb.append(", currentScrollId='").append(currentScrollId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public boolean isScrolling() {
        return currentScrollId.isPresent();
    }

    @Override
    public List<SourceRecord> getRecords(final int batchMaxRows) {
        final List<SourceRecord> records = new ArrayList<>(batchMaxRows);

        try {
            final ElasticsearchScrollResponse response;
            if (currentScrollId.isPresent()) {
                response = elasticsearchDAO.continueScroll(currentScrollId.get());
            } else {
                final boolean emptyIndex = (!incrementingFieldFirstValue.isPresent());
                if (emptyIndex) {
                    // let's check again for new documents on index
                    incrementingFieldFirstValue = getFirstFieldValueFromIndex();
                    if(!incrementingFieldFirstValue.isPresent()) {
                        return Collections.emptyList();
                    }
                }

                final boolean firstRunOnNewIndex =
                        incrementingFieldFirstValue.isPresent()
                                && !incrementingFieldLastValue.isPresent();
                if (firstRunOnNewIndex) {
                    logger.info("Start scrolling from the beginning of the index, oldest value found: {}", incrementingFieldFirstValue.get());
                    incrementingFieldLastValue = Optional.of(incrementingFieldFirstValue.get());
                }

                response = elasticsearchDAO.startScroll(
                        indexName,
                        incrementingFieldName,
                        incrementingFieldLastValue.get(),
                        firstRunOnNewIndex, // Include the oldest element in case of first run on new index
                        batchMaxRows);
            }

            final SearchHits hits = response.getHits();
            this.currentScrollId = Optional.of(response.getScrollId());
            logger.debug("Retrieved {} hits and scroll id: {}", hits.totalHits, response.getScrollId());

            for (SearchHit hit : hits.getHits()) {
                final Map<String, Object> documentAsMap = hit.getSourceAsMap();
                final Map<String, String> keyMapForOffsetsTopic = Utils.generateKeyForOffsetsTopic(indexName);
                final Map<String, String> valueMapForOffsetsTopic = Utils.generateValueForOffsetsTopic(
                        documentAsMap.get(incrementingFieldName).toString()
                );

                final Schema schema = SchemaConverter.buildSchemaForDocument(documentAsMap, indexName);
                final Struct struct = StructConverter.buildStructForDocument(documentAsMap, schema);

                // Document key
                // TODO: make key flexible
                final String key = String.join("_", hit.getIndex(), hit.getType(), hit.getId());

                SourceRecord sourceRecord = new SourceRecord(
                        keyMapForOffsetsTopic,
                        valueMapForOffsetsTopic,
                        targetTopic,
                        Schema.STRING_SCHEMA,   // TODO: make key flexible
                        key,
                        schema, // Value
                        struct);
                records.add(sourceRecord);

                incrementingFieldLastValue = Optional.of(documentAsMap.get(incrementingFieldName).toString());
            }
            return records;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
