package com.rbkmoney.eventstock.client.poll;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;

/**
 * Created by vpankrashkin on 14.07.16.
 */
public class TemporalConverterTest {
    @Test
    public void testConvertation() {
        String timeStr = "2016-03-22T06:12:27Z";
        Instant instant = Instant.from(TemporalConverter.stringToTemporal(timeStr));

        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC.normalized());
        assertEquals(2016, ldt.getYear());
        assertEquals(6, ldt.getHour());
        assertEquals(12, ldt.getMinute());
        assertEquals(27, ldt.getSecond());

        assertEquals(timeStr, TemporalConverter.temporalToString(instant));
    }
}
