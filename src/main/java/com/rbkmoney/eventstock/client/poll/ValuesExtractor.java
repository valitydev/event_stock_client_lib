package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.SourceEvent;
import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.damsel.payment_processing.Event;
import com.rbkmoney.eventstock.client.poll.TemporalConverter;

import java.time.temporal.TemporalAccessor;
import java.util.Optional;

/**
 * Created by vpankrashkin on 12.07.16.
 */
class ValuesExtractor {
    public static Long getEventId(StockEvent event) {
        return Optional.of(event).map(StockEvent::getSourceEvent).map(SourceEvent::getProcessingEvent).map(Event::getId).orElse(null);
    }

    public static TemporalAccessor getCreatedAt(StockEvent event) {
        String createdAtStr = Optional.of(event).map(StockEvent::getSourceEvent).map(SourceEvent::getProcessingEvent).map(Event::getCreatedAt).orElse(null);
        return TemporalConverter.stringToTemporal(createdAtStr);
    }
}
