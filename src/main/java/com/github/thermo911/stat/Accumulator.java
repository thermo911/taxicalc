package com.github.thermo911.stat;

/**
 * Trivial interface of accumulator.
 * @param <V> type of values being accumulated
 * @param <R> type of result of accumulation
 */
public interface Accumulator<V, R> {

    void accept(V value);

    R getResult();
}
