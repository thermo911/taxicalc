package com.github.thermo911.stat;

public class MinLongAccumulator implements Accumulator<Long, Long> {
    private Long min = null;

    @Override
    public void accept(Long value) {
        if (min == null || min < value) {
            min = value;
        }
    }

    @Override
    public Long getResult() {
        return min;
    }
}
