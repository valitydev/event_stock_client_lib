package com.rbkmoney.bmclient.polling;

import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.event_stock.StockEvent;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalField;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public class BMRangeChecker {
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_INSTANT;

    public boolean isInRange(StockEvent event, EventRange range) {

        long id = event.getSourceEvent().getProcessingEvent().getId();
        boolean result = range.getIdRange().getFromId().isSetInclusive() ? id >= range.getIdRange().getFromId().getInclusive() : id > range.getIdRange().getFromId().getExclusive();
        result &= range.getIdRange().getToId().isSetInclusive() ? id <= range.getIdRange().getToId().getInclusive() : id < range.getIdRange().getToId().getExclusive();

        return result;
    }
}
