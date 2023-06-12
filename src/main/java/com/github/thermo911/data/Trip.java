package com.github.thermo911.data;

import java.time.Instant;

public record Trip(int passengerCount,
                   double tripDistance,
                   long pickupTimeMs,
                   long dropoffTimeMs) {

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
