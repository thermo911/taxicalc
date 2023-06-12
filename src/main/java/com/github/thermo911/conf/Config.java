package com.github.thermo911.conf;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;

public final class Config {
    private Config() {
        throw new UnsupportedOperationException("Utility class!");
    }

    public static final String PASSENGER_COUNT_COLUMN = "passenger_count";
    public static final String TRIP_DISTANCE_COLUMN = "trip_distance";
    public static final String PICKUP_DATETIME_COLUMN = "tpep_pickup_datetime";
    public static final String DROPOFF_DATETIME_COLUMN = "tpep_dropoff_datetime";

    public static final Set<String> COLUMN_NAMES = Set.of(
            PASSENGER_COUNT_COLUMN,
            TRIP_DISTANCE_COLUMN,
            PICKUP_DATETIME_COLUMN,
            DROPOFF_DATETIME_COLUMN
    );

    public static final String PARQUET_FILE_EXTENSION = ".parquet";

    public static final Configuration HADOOP_CONFIG = new Configuration();
}
