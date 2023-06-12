package com.github.thermo911.stat;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.github.thermo911.data.Trip;

public class TripAccumulator implements Accumulator<Trip, Map<Integer, Double>> {

    private final Map<Integer, AverageAccumulator> accumulators = new HashMap<>();

    @Override
    public void accept(Trip value) {
        System.out.println(value);
        accumulators.compute(value.passengerCount(), (k, v) -> {
            if (v == null) {
                v = new AverageAccumulator();
            }
            v.accept(value.tripDistance());
            return v;
        });
    }

    @Override
    public Map<Integer, Double> getResult() {
        return accumulators.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getResult()
                ));
    }
}
