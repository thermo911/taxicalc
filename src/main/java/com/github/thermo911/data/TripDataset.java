package com.github.thermo911.data;

import java.io.Closeable;
import java.time.LocalDateTime;
import java.util.Iterator;

public interface TripDataset extends Closeable {
    /**
     * Returns iterator for iterating over trips fitting given time constraints.
     * @param start time of trip start (inclusively)
     * @param end   time of trip end (inclusively)
     * @return the iterator
     */
    Iterator<Trip> trips(LocalDateTime start, LocalDateTime end);
}
