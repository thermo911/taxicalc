package com.github.thermo911.stat;

public class AverageAccumulator implements Accumulator<Double, Double> {
    private int count = 0;
    private double sum = 0.0;

    @Override
    public void accept(Double value) {
        sum += value;
        count++;
    }

    @Override
    public Double getResult() {
        return sum / count;
    }
}
