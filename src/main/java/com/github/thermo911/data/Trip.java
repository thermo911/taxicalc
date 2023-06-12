package com.github.thermo911.data;

import java.time.Instant;
import java.time.LocalDateTime;

import static com.github.thermo911.util.Utils.toEpochMillis;

public record Trip(int passengerCount,
                   double tripDistance,
                   long pickupTimeMs,
                   long dropoffTimeMs) {

    public Trip(int passengerCount, double tripDistance, LocalDateTime pickupTime, LocalDateTime dropoffTime) {
        this(passengerCount, tripDistance, toEpochMillis(pickupTime), toEpochMillis(dropoffTime));
    }

    @Override
    public String toString() {
        return "Trip{" +
                "passengerCount=" + passengerCount +
                ", tripDistance=" + tripDistance +
                ", pickupTime=" + Instant.ofEpochMilli(pickupTimeMs / 1000L) +
                ", dropoffTime=" + Instant.ofEpochMilli(dropoffTimeMs / 1000L) +
                '}';
    }
}
