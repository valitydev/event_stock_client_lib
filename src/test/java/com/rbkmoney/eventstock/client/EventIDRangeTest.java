package com.rbkmoney.eventstock.client;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static com.rbkmoney.eventstock.client.EventConstraint.*;

/**
 * Created by vpankrashkin on 14.07.16.
 */
@RunWith(Parameterized.class)
public class EventIDRangeTest {
    private EventConstraint.EventIDRange range1;
    private EventConstraint.EventIDRange range2;
    private boolean expected;

    public EventIDRangeTest(EventConstraint.EventIDRange range1, EventConstraint.EventIDRange range2, boolean expected) {
        this.range1 = range1;
        this.range2 = range2;
        this.expected = expected;
    }

    @Test
    public void testIntersected() {
        assertTrue(range1.isIntersected(range1));
        assertTrue(range2.isIntersected(range2));
        assertEquals(expected, range1.isIntersected(range2));
        assertEquals(expected, range2.isIntersected(range1));
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {new EventIDRange(0L, true, 10L, false) , new EventIDRange(0L, true, 10L, false), true},
                {new EventIDRange(1L, false, 2L, false) , new EventIDRange(1L, true, 2L, true), true},
                {new EventIDRange(0L, true, 2L, false) , new EventIDRange(1L, true, 2L, false), true},
                {new EventIDRange(0L, true, 3L, false) , new EventIDRange(1L, true, 2L, true), true},
                {new EventIDRange(0L, true, 2L, false) , new EventIDRange(2L, true, 3L, false), false},
                {new EventIDRange(0L, true, 2L, false) , new EventIDRange(-2L, true, -1L, true), false},
                {new EventIDRange(0L, true, 2L, false) , new EventIDRange(-2L, true, 0L, true), true},
                {new EventIDRange(0L, true, 2L, true) , new EventIDRange(2L, true, 3L, true), true},
                {new EventIDRange(0L, true, 2L, true) , new EventIDRange(20L, true, 30L, true), false},
                {new EventIDRange(0L, true, 2L, true) , new EventIDRange(-30L, true, -20L, true), false},
                {new EventIDRange(0L, true, 20L, true) , new EventIDRange(-3L, true, 2L, true), true},
                {new EventIDRange(0L, true, 20L, true) , new EventIDRange(5L, true, 22L, true), true},
                {new EventIDRange(0L, true, 20L, true) , new EventIDRange(5L, true, 7L, true), true},
        });
    }
}
