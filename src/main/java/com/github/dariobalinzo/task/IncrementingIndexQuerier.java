package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import com.github.dariobalinzo.elasticsearch.ElasticsearchScrollResponse;
import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.unit.TimeValue;
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
                logger.info("Oldest value for field: {} on index: {} is: {}", this.incrementingFieldName, this.indexName, this.incrementingFieldFirstValue);
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
                final Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                final Map sourcePartition = Collections.singletonMap(INDEX, indexName);
                final Map sourceOffset = Collections.singletonMap(POSITION, sourceAsMap.get(incrementingFieldName).toString());

                Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(sourceAsMap, indexName);
                Struct struct = StructConverter.convertElasticDocument2AvroStruct(sourceAsMap, schema);

                // Document key
                String key = String.join("_", hit.getIndex(), hit.getType(), hit.getId());

                SourceRecord sourceRecord = new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        targetTopic,
                        //KEY
                        Schema.STRING_SCHEMA,
                        key,
                        //VALUE
                        schema,
                        struct);
                results.add(sourceRecord);

                last.put(index,sourceAsMap.get(incrementingField).toString());
                sent.merge(index, 1, Integer::sum);
            }



            // TODO: do something with items and add them to results
            // TODO: remember the last one found in memory
            return records;
        } catch (IOException e) {
            // TODO: in case scroll or connection error, try to invalidate it and try again later
            return Collections.emptyList();
        }
    }


    @Override
    public SourceRecord extractRecord(SearchHit hit) {
        Map<String, Object> sourceAsMap = hit.getSourceAsMap();

        // TODO: capire cosa fa qui
        Map sourcePartition = Collections.singletonMap(INDEX, indexName);
        Map sourceOffset = Collections.singletonMap(POSITION, sourceAsMap.get(incrementingFieldName).toString());

        // TODO: capire cosa fa con lo schema
        Schema valueSchema = SchemaConverter.convertElasticMapping2AvroSchema(sourceAsMap, indexName);
        Struct struct = StructConverter.convertElasticDocument2AvroStruct(sourceAsMap, valueSchema);

        // TODO: define key & topic strategy
        final String key = String.join("_", hit.getIndex(), hit.getType(), hit.getId());
        final String topic = topicPrefix + indexName;

        SourceRecord sourceRecord = new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                Schema.STRING_SCHEMA, // Key schema
                key,
                valueSchema,          // Value schema
                struct);

        // TODO: capire cosa fa qui
        last.put(indexName, sourceAsMap.get(incrementingFieldName).toString());
        sent.merge(indexName, 1, Integer::sum);
        return sourceRecord;
    }

}
