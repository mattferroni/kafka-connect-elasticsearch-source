package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import com.github.dariobalinzo.elasticsearch.ElasticsearchScrollResponse;
import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import com.github.dariobalinzo.utils.ElasticSearchDataProvider;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class IncrementingIndexQuerier extends IndexQuerier {

    static final Logger logger = LoggerFactory.getLogger(IncrementingIndexQuerier.class);

    private final String incrementingFieldName;

    // Mutable state
    private Optional<String> lastIncrementingFieldValue;

    public IncrementingIndexQuerier(
            ElasticsearchDAO elasticsearchDAO,
            String indexName,
            String topicPrefix,
            String incrementingFieldName,
            Map<String, Object> offset      // TODO: make this a value, no offset logic in here
    ) {
        super(elasticsearchDAO, indexName, topicPrefix);
        this.incrementingFieldName = incrementingFieldName;

        if (offset != null && !offset.isEmpty()) {
            initLastFieldValueFrom(offset);
        } else {
            initLastFieldValueFromIndex();
        }

        if (lastIncrementingFieldValue.isPresent()) {
            logger.info("Found last {} value: {} for index: {}", this.incrementingFieldName, this.lastIncrementingFieldValue, this.indexName);
        } else {
            logger.warn("Not able to find a value for field: {} on index: {}", this.incrementingFieldName, this.indexName);
        }
    }

    private void initLastFieldValueFrom(Map<String, Object> offset) {
        // TODO: use something like this
        logger.debug("Looking for committed offsets for the index: {}", indexName);
        //if cache is empty we check the framework
        // Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(INDEX, index));
        // if (offset != null) {
        //     return (String) offset.get(POSITION);
        lastIncrementingFieldValue = Optional.empty();
    }

    private void initLastFieldValueFromIndex() {
        try {
            logger.debug("Looking for the oldest document in the index: {}", indexName);
            lastIncrementingFieldValue = elasticsearchDAO.getFirstValue(indexName, incrementingFieldName);
        } catch (IOException e) {
            logger.error("Exception while looking for the oldest document", e);
        }
    }

    @Override
    public String toString() {
        // TODO: implement this
        final StringBuffer sb = new StringBuffer("IncrementingIndexQuerier{");
        sb.append('}');
        return sb.toString();
    }

    @Override
    public List<SourceRecord> getRecords(final int batchMaxRows) {
        final List<SourceRecord> records = new ArrayList<>(batchMaxRows);

        try {
            final ElasticsearchScrollResponse response;
            if (this.isScrolling()) {
                response = elasticsearchDAO.continueScroll(currentScrollId);
            } else {
                // I assume no offsets found
                if (!lastIncrementingFieldValue.isPresent()) {
                    // let's check again for new documents on index
                    initLastFieldValueFromIndex();
                    if (!lastIncrementingFieldValue.isPresent()) {
                        return Collections.emptyList();
                    }
                }

                response = elasticsearchDAO.startScroll(
                        indexName,
                        incrementingFieldName,
                        lastIncrementingFieldValue.get(),
                        true,       // FIXME: is this ok?
                        batchMaxRows);
            }

            // TODO: do something with items and add them to results
            // TODO: remember the last one found in memory
            return records;
        } catch (IOException e) {
            // TODO: in case scroll or connection error, try to invalidate it and try again later
            return Collections.emptyList();
        }
    }

    public List<SourceRecord> executeQuery(final ElasticSearchDataProvider esProvider, final long batchMaxRows) {


        SearchHits hits = searchResponse.getHits();
        logger.debug("Retrieved {} hits and scroll id: {}", hits, scrollId);
        // TODO: check valid scrollId

        SearchHit[] searchHits = parseSearchResult(indexName, lastIncrementingFieldValue.get(), results, searchResponse, scrollId);






        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {

        }
        return searchHits;
























        while (searchHits != null && searchHits.length > 0 && results.size() < size) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
            searchResponse = elasticConnectionProvider.getClient().searchScroll(scrollRequest);
            scrollId = searchResponse.getScrollId();
            searchHits = parseSearchResult(index, lastValue, results, searchResponse, scrollId);
        }


        try {





        } catch (Throwable t) {
            logger.error("error", t);
        } finally {
            closeScrollQuietly(scrollId);
        }
        return null;
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
