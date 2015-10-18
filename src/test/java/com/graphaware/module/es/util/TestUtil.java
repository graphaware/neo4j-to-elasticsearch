package com.graphaware.module.es.util;

public final class TestUtil {

    private TestUtil() {
    }

    public static void waitFor(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
