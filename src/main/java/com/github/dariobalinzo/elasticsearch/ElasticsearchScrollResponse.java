package com.github.dariobalinzo.elasticsearch;

import org.elasticsearch.search.SearchHits;

public class ElasticsearchScrollResponse {

    private final SearchHits hits;
    private final String scrollId;

    public ElasticsearchScrollResponse(SearchHits hits, String scrollId) {
        this.hits = hits;
        this.scrollId = scrollId;
    }

    public SearchHits getHits() {
        return hits;
    }

    public String getScrollId() {
        return scrollId;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ElasticsearchScrollResponse{");
        sb.append("hits count=").append(hits.getHits().length);
        sb.append(", scrollId='").append(scrollId).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
