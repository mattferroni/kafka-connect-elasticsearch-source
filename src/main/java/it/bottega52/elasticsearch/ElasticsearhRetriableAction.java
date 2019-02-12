package it.bottega52.elasticsearch;

import java.io.IOException;

/**
 * Whatever action we might want to retry for a certain number of times
 *
 * @param <R>
 * @param <E>
 */
interface ElasticsearhRetriableAction<R,E extends IOException> {
    public R apply() throws E;
}
