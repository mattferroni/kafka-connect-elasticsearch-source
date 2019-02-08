package com.github.dariobalinzo;

import com.github.dariobalinzo.elasticsearch.ElasticsearchDAO;
import junit.framework.TestCase;

public class TestElasticsearchDAO extends TestCase {

    private static final String HOST = "localhost";
    private static final int PORT = 9200;
    private static final int MAX_CONNECTION_ATTEMPTS = 5;
    private static final long CONNECTION_RETRY_BACKOFF = 1000;
    private static final String PREFIX = "jago_prod";

    private ElasticsearchDAO dao;

    public void setUp() throws Exception {
        dao = new ElasticsearchDAO(HOST, PORT, MAX_CONNECTION_ATTEMPTS, CONNECTION_RETRY_BACKOFF);
    }

    public void testConnection() throws Exception {
        final boolean result = dao.testConnection();
        assertEquals(result, true);
    }

    /**
     * other tests
     */

    /*
     SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices("metricbeat-6.2.4-2018.05.20");
        SearchResponse searchResponse = es.getClient().search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
            Schema schema = SchemaConverter.buildSchemaForDocument(sourceAsMap, "test");
            schema.toString();
            Struct struct = StructConverter.buildStructForDocument(sourceAsMap,schema);
            struct.toString();
        }
     */

    /*
    public void testTask() throws Exception {

        task.setupTest(Arrays.asList("metricbeat-6.2.4-2018.05.20", "metricbeat-6.2.4-2018.05.21", "metricbeat-6.2.4-2018.05.22"));
        //while (true) {
        //    task.poll();
        //}
        //List list = new ArrayList<>();
        //task.executeScroll("metricbeat-6.2.4-2018.05.20",
        //        "2018-05-20T10:26:07.747Z",list);
        //list.size();

    }
     */

}
