package com.rackspacecloud.metrics.kafkainfluxdbconsumer.influxdb;

public final class Preconditions {
    private Preconditions() {
    }

    public static String checkNonEmptyString(String string, String name) throws IllegalArgumentException {
        if (string != null && !string.isEmpty()) {
            return string;
        } else {
            throw new IllegalArgumentException("Expecting a non-empty string for " + name);
        }
    }

    public static void checkPositiveNumber(Number number, String name) throws IllegalArgumentException {
        if (number == null || number.doubleValue() <= 0.0D) {
            throw new IllegalArgumentException("Expecting a positive number for " + name);
        }
    }

    public static void checkNotNegativeNumber(Number number, String name) throws IllegalArgumentException {
        if (number == null || number.doubleValue() < 0.0D) {
            throw new IllegalArgumentException("Expecting a positive or zero number for " + name);
        }
    }

    public static void checkDuration(String duration, String name) throws IllegalArgumentException {
        if (!duration.matches("(\\d+[wdmhs])+")) {
            throw new IllegalArgumentException("Invalid InfluxDB duration: " + duration + "for " + name);
        }
    }
}
