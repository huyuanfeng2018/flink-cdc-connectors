/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.convertTimestampToLocalDateTime;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.splitKeyRangeContains;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils}. */
class RecordUtilsTest {

    @Test
    void testSplitKeyRangeContains() {
        // table with only one split
        assertKeyRangeContains(new Object[] {100L}, null, null);
        // the last split
        assertKeyRangeContains(new Object[] {101L}, new Object[] {100L}, null);

        // the first split
        assertKeyRangeContains(new Object[] {101L}, null, new Object[] {1024L});

        // general splits
        assertKeyRangeContains(new Object[] {100L}, new Object[] {1L}, new Object[] {1024L});
        Assertions.assertThat(
                        splitKeyRangeContains(
                                new Object[] {0L}, new Object[] {1L}, new Object[] {1024L}))
                .isFalse();

        // split key from binlog may have different type
        assertKeyRangeContains(
                new Object[] {BigInteger.valueOf(100L)}, new Object[] {1L}, new Object[] {1024L});
        Assertions.assertThat(
                        splitKeyRangeContains(
                                new Object[] {BigInteger.valueOf(0L)},
                                new Object[] {1L},
                                new Object[] {1024L}))
                .isFalse();
    }

    @Test
    void testDifferentKeyTypes() {
        // first split
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Byte.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Short.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Integer.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {Long.valueOf("6")});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {BigInteger.valueOf(6)});
        assertKeyRangeContains(new Object[] {5}, null, new Object[] {BigDecimal.valueOf(6)});

        // other splits
        assertKeyRangeContains(
                new Object[] {Byte.valueOf("6")},
                new Object[] {Byte.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Short.valueOf("60")},
                new Object[] {Short.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Integer.valueOf("600")},
                new Object[] {Integer.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {Long.valueOf("6000")},
                new Object[] {Long.valueOf("6")},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {BigInteger.valueOf(60000)},
                new Object[] {BigInteger.valueOf(6)},
                new Object[] {BigDecimal.valueOf(100000L)});
        assertKeyRangeContains(
                new Object[] {BigDecimal.valueOf(60000)},
                new Object[] {BigDecimal.valueOf(6)},
                new Object[] {BigDecimal.valueOf(100000L)});

        // last split
        assertKeyRangeContains(new Object[] {7}, new Object[] {Byte.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Short.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Integer.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {Long.valueOf("6")}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {BigInteger.valueOf(6)}, null);
        assertKeyRangeContains(new Object[] {7}, new Object[] {BigDecimal.valueOf(6)}, null);
    }

    private void assertKeyRangeContains(
            Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        Assertions.assertThat(splitKeyRangeContains(key, splitKeyStart, splitKeyEnd)).isTrue();
    }

    @Test
    void testTimestampConversionConsistency() throws Exception {
        // Test DATETIME types with different precisions
        testDateTimeConversion(0); // DATETIME(0) - second precision
        testDateTimeConversion(3); // DATETIME(3) - millisecond precision
        testDateTimeConversion(6); // DATETIME(6) - microsecond precision

        // Test TIMESTAMP types with different precisions
        testTimestampConversion(0); // TIMESTAMP(0)
        testTimestampConversion(3); // TIMESTAMP(3)
        testTimestampConversion(6); // TIMESTAMP(6)
    }

    /**
     * Tests DATETIME conversion (uses UTC timezone).
     *
     * <p>Simulates Debezium's conversion: LocalDateTime -> Long -> LocalDateTime and verifies
     * consistency.
     */
    private void testDateTimeConversion(int precision) {
        ZoneId utc = ZoneOffset.UTC;
        LocalDateTime original = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456789);

        // MySQL will truncate the value based on precision before storing
        LocalDateTime truncatedOriginal;
        if (precision == 0) {
            truncatedOriginal = original.truncatedTo(ChronoUnit.SECONDS);
        } else if (precision <= 3) {
            truncatedOriginal = original.truncatedTo(ChronoUnit.MILLIS);
        } else {
            truncatedOriginal = original.truncatedTo(ChronoUnit.MICROS);
        }

        // Simulate Debezium's conversion: LocalDateTime -> Long
        long timestampFromDebezium;
        if (precision > 3) {
            // Microseconds precision
            timestampFromDebezium =
                    ChronoUnit.MICROS.between(
                            Instant.EPOCH, truncatedOriginal.atZone(utc).toInstant());
        } else {
            // Milliseconds precision (also used for precision 0)
            timestampFromDebezium = truncatedOriginal.atZone(utc).toInstant().toEpochMilli();
        }

        // Our reverse conversion: Long -> LocalDateTime
        LocalDateTime converted =
                convertTimestampToLocalDateTime(timestampFromDebezium, precision, false, utc);

        // Verify consistency: converted should equal truncatedOriginal
        assertThat(converted)
                .isEqualTo(truncatedOriginal)
                .withFailMessage(
                        "DATETIME(%d) conversion failed. Expected: %s, Converted: %s",
                        precision, truncatedOriginal, converted);
    }

    /**
     * Tests TIMESTAMP conversion (uses server timezone).
     *
     * <p>Simulates Debezium's conversion with server timezone.
     */
    private void testTimestampConversion(int precision) throws Exception {
        // Test with different timezones
        ZoneId serverTimeZone = ZoneId.of("Asia/Shanghai");
        LocalDateTime original = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456789);

        // MySQL will truncate the value based on precision before storing
        LocalDateTime truncatedOriginal;
        if (precision == 0) {
            truncatedOriginal = original.truncatedTo(ChronoUnit.SECONDS);
        } else if (precision <= 3) {
            truncatedOriginal = original.truncatedTo(ChronoUnit.MILLIS);
        } else {
            truncatedOriginal = original.truncatedTo(ChronoUnit.MICROS);
        }

        // Simulate Debezium's conversion with server timezone
        long timestampFromDebezium;
        if (precision > 3) {
            timestampFromDebezium =
                    ChronoUnit.MICROS.between(
                            Instant.EPOCH, truncatedOriginal.atZone(serverTimeZone).toInstant());
        } else {
            timestampFromDebezium =
                    truncatedOriginal.atZone(serverTimeZone).toInstant().toEpochMilli();
        }

        // Our reverse conversion
        LocalDateTime converted =
                convertTimestampToLocalDateTime(
                        timestampFromDebezium, precision, true, serverTimeZone);

        // Verify consistency: converted should equal truncatedOriginal
        assertThat(converted)
                .isEqualTo(truncatedOriginal)
                .withFailMessage(
                        "TIMESTAMP(%d) conversion failed. Expected: %s, Converted: %s",
                        precision, truncatedOriginal, converted);
    }

    @Test
    void testEdgeCases() {
        ZoneId utc = ZoneOffset.UTC;

        // Test null timestamp
        LocalDateTime nullResult = convertTimestampToLocalDateTime(null, 3, false, utc);
        assertThat(nullResult).isNull();

        // Test epoch (1970-01-01 00:00:00)
        LocalDateTime epoch = Instant.ofEpochSecond(0, 0).atZone(utc).toLocalDateTime();
        long epochMillis = 0L;
        LocalDateTime convertedEpoch = convertTimestampToLocalDateTime(epochMillis, 3, false, utc);
        assertThat(convertedEpoch).isEqualTo(epoch);

        // Test far future date
        LocalDateTime future = LocalDateTime.of(2099, 12, 31, 23, 59, 59, 999999000);
        long futureMicros =
                ChronoUnit.MICROS.between(Instant.EPOCH, future.atZone(utc).toInstant());
        LocalDateTime convertedFuture =
                convertTimestampToLocalDateTime(futureMicros, 6, false, utc);
        assertThat(convertedFuture).isEqualTo(future.truncatedTo(ChronoUnit.MICROS));
    }

    @Test
    void testPrecisionBoundary() {
        ZoneId utc = ZoneOffset.UTC;
        LocalDateTime testTime = LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123456789);

        // Test precision = 3 boundary (milliseconds)
        long millis = testTime.atZone(utc).toInstant().toEpochMilli();
        LocalDateTime result3 = convertTimestampToLocalDateTime(millis, 3, false, utc);
        assertThat(result3.getNano() % 1_000_000).isEqualTo(0); // Should be millisecond aligned

        // Test precision = 4 boundary (microseconds)
        long micros = ChronoUnit.MICROS.between(Instant.EPOCH, testTime.atZone(utc).toInstant());
        LocalDateTime result4 = convertTimestampToLocalDateTime(micros, 4, false, utc);
        assertThat(result4.getNano() % 1_000).isEqualTo(0); // Should be microsecond aligned
    }
}
