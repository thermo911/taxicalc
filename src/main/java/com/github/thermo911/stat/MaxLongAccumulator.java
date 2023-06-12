package com.github.thermo911.stat;

public class MaxLongAccumulator implements Accumulator<Long, Long> {
    private Long max = null;

    @Override
    public void accept(Long value) {
        if (max == null || max < value) {
            max = value;
        }
    }

    @Override
    public Long getResult() {
        return max;
    }
}
