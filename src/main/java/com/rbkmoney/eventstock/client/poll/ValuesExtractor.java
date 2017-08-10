package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.geck.common.util.TypeUtil;

import java.time.temporal.TemporalAccessor;

/**
 * Created by vpankrashkin on 12.07.16.
 */
class ValuesExtractor {
    public static Long getEventId(StockEvent event) {
        if (event == null) {
            return null;
        }
        if (event.getSourceEvent().isSetProcessingEvent()) {
            return event.getSourceEvent().getProcessingEvent().getId();
        } else if (event.getSourceEvent().isSetPayoutEvent()) {
            return event.getSourceEvent().getPayoutEvent().getId();
        }
        return null;
    }

    public static TemporalAccessor getCreatedAt(StockEvent event) {
        if (event == null) {
            return null;
        }
        String time = null;
        if (event.getSourceEvent().isSetProcessingEvent()) {
            time = event.getSourceEvent().getProcessingEvent().getCreatedAt();
        } else if (event.getSourceEvent().isSetPayoutEvent()) {
            time = event.getSourceEvent().getPayoutEvent().getCreatedAt();
        }
        return time != null ? TypeUtil.stringToTemporal(time) : null;
    }
}
