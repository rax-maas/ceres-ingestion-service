package com.rackspacecloud.metrics.ingestionservice.influxdb;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Point {
    private String measurement;
    private Map<String, String> tags;
    private Long time;
    private TimeUnit precision;
    private Map<String, Object> fields;

    private static final int MAX_FRACTION_DIGITS = 340;

    private static final ThreadLocal<NumberFormat> NUMBER_FORMATTER = ThreadLocal.withInitial(() -> {
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
        numberFormat.setMaximumFractionDigits(MAX_FRACTION_DIGITS);
        numberFormat.setGroupingUsed(false);
        numberFormat.setMinimumFractionDigits(1);
        return numberFormat;
    });

    Point() {
        this.precision = TimeUnit.NANOSECONDS;
    }

    public static Point.Builder measurement(String measurement) {
        return new Point.Builder(measurement);
    }

    public String lineProtocol(TimeUnit precision) {
        StringBuilder sb = new StringBuilder(measurement);
        this.concatenatedTags(sb);
        this.concatenatedFields(sb);
        this.formattedTime(sb, precision);
        return sb.toString();
    }

    private StringBuilder formattedTime(StringBuilder sb, TimeUnit precision) {
        if (this.time != null && this.precision != null) {
            sb.append(" ").append(precision.convert(this.time, this.precision));
            return sb;
        } else {
            return sb;
        }
    }

    private void concatenatedFields(StringBuilder sb) {
        Iterator iterator = this.fields.entrySet().iterator();

        while(true) {
            Map.Entry field;
            Object value;
            do {
                if (!iterator.hasNext()) {
                    int lengthMinusOne = sb.length() - 1;
                    if (sb.charAt(lengthMinusOne) == ',') {
                        sb.setLength(lengthMinusOne);
                    }

                    return;
                }

                field = (Map.Entry)iterator.next();
                value = field.getValue();
            } while(value == null);

            escapeKey(sb, (String)field.getKey());
            sb.append('=');
            if (value instanceof Number) {
                if (!(value instanceof Double) && !(value instanceof Float) && !(value instanceof BigDecimal)) {
                    sb.append(value).append('i');
                } else {
                    sb.append(NUMBER_FORMATTER.get().format(value));
                }
            } else if (value instanceof String) {
                String stringValue = (String)value;
                sb.append('"');
                escapeField(sb, stringValue);
                sb.append('"');
            } else {
                sb.append(value);
            }

            sb.append(',');
        }
    }

    private void concatenatedTags(StringBuilder sb) {
        Iterator iterator = this.tags.entrySet().iterator();

        while(iterator.hasNext()) {
            Map.Entry<String, String> tag = (Map.Entry)iterator.next();
            sb.append(',');
            escapeKey(sb, tag.getKey());
            sb.append('=');
            escapeKey(sb, tag.getValue());
        }

        sb.append(' ');
    }

    void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    void setTime(Long time) {
        this.time = time;
    }

    void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    Map<String, String> getTags() {
        return this.tags;
    }

    void setPrecision(TimeUnit precision) {
        this.precision = precision;
    }

    void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    static void escapeKey(StringBuilder sb, String key) {
        int i = 0;

        while(i < key.length()) {
            switch(key.charAt(i)) {
                case ' ':
                case ',':
                case '=':
                    sb.append('\\');
                default:
                    sb.append(key.charAt(i));
                    ++i;
            }
        }
    }

    static void escapeField(StringBuilder sb, String field) {
        int i = 0;

        while(i < field.length()) {
            switch(field.charAt(i)) {
                case '"':
                case '\\':
                    sb.append('\\');
                default:
                    sb.append(field.charAt(i));
                    ++i;
            }
        }
    }

    public static final class Builder {
        private final String measurement;
        private final Map<String, String> tags = new TreeMap<>();
        private Long time;
        private TimeUnit precision;
        private final Map<String, Object> fields = new TreeMap();

        Builder(String measurement) {
            this.measurement = measurement;
        }

        public Point.Builder tag(String tagName, String value) {
            Objects.requireNonNull(tagName, "tagName");
            Objects.requireNonNull(value, "value");
            if (!tagName.isEmpty() && !value.isEmpty()) {
                this.tags.put(tagName, value);
            }

            return this;
        }

        public Point.Builder tag(Map<String, String> tagsToAdd) {
            Iterator var2 = tagsToAdd.entrySet().iterator();

            while(var2.hasNext()) {
                Map.Entry<String, String> tag = (Map.Entry<String, String>) var2.next();
                this.tag((String)tag.getKey(), (String)tag.getValue());
            }

            return this;
        }

        public Point.Builder addField(String field, boolean value) {
            this.fields.put(field, value);
            return this;
        }

        public Point.Builder addField(String field, long value) {
            this.fields.put(field, value);
            return this;
        }

        public Point.Builder addField(String field, double value) {
            this.fields.put(field, value);
            return this;
        }

        public Point.Builder addField(String field, Number value) {
            this.fields.put(field, value);
            return this;
        }

        public Point.Builder addField(String field, String value) {
            Objects.requireNonNull(value, "value");
            this.fields.put(field, value);
            return this;
        }

        public Point.Builder fields(Map<String, Object> fieldsToAdd) {
            this.fields.putAll(fieldsToAdd);
            return this;
        }

        public Point.Builder time(long timeToSet, TimeUnit precisionToSet) {
            Objects.requireNonNull(precisionToSet, "precisionToSet");
            this.time = timeToSet;
            this.precision = precisionToSet;
            return this;
        }

        public boolean hasFields() {
            return !this.fields.isEmpty();
        }

        public Point build() {
            Preconditions.checkNonEmptyString(this.measurement, "measurement");
            Preconditions.checkPositiveNumber(this.fields.size(), "fields size");
            Point point = new Point();
            point.setFields(this.fields);
            point.setMeasurement(this.measurement);
            if (this.time != null) {
                point.setTime(this.time);
                point.setPrecision(this.precision);
            }

            point.setTags(this.tags);
            return point;
        }
    }
}
