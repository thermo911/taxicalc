package com.github.thermo911.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public final class Utils {
    private Utils() {
        throw new UnsupportedOperationException("Utility class!");
    }

    public static long toEpochMillis(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    }
}
