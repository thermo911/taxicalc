package com.github.thermo911.parquet;

import com.github.thermo911.conf.Config;
import com.github.thermo911.data.Trip;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * Class for materializing trips from stream of Parquet data.
 */
class TripConverter extends RecordMaterializer<Trip> {
    /**
     * The result of "bad" data materialization.
     */
    public static final Trip BROKEN_TRIP = new Trip(-1, -1.0, -1L, -1L);

    private final GroupRecordConverter root;

    public TripConverter(MessageType schema) {
        root = new GroupRecordConverter(schema);
    }

    @Override
    public Trip getCurrentRecord() {
        Group group = root.getCurrentRecord();
        try {
            int passengerCount = (int) group.getDouble(Config.PASSENGER_COUNT_COLUMN, 0);
            double distance = group.getDouble(Config.TRIP_DISTANCE_COLUMN, 0);
            long pickupTimeMs = group.getLong(Config.PICKUP_DATETIME_COLUMN, 0);
            long dropoffTimeMs = group.getLong(Config.DROPOFF_DATETIME_COLUMN, 0);
            if (pickupTimeMs > dropoffTimeMs) {
                return BROKEN_TRIP;
            }
            return new Trip(passengerCount, distance, pickupTimeMs, dropoffTimeMs);
        } catch (Exception e) {
            return BROKEN_TRIP;
        }
    }

    @Override
    public GroupConverter getRootConverter() {
        return root.getRootConverter();
    }
}
