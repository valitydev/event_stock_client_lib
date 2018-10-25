package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.geck.common.util.TypeUtil;

import java.time.temporal.TemporalAccessor;

/**
 * Created by vpankrashkin on 12.07.16.
 */
class ValuesExtractor {
    public static <T> Long getEventId(T event) {
        if (event == null) {
            return null;
        }
        if (event instanceof StockEvent) {
            StockEvent stockEvent = (StockEvent) event;
            if (stockEvent.getSourceEvent().isSetProcessingEvent()) {
                return stockEvent.getSourceEvent().getProcessingEvent().getId();
            } else if (stockEvent.getSourceEvent().isSetPayoutEvent()) {
                return stockEvent.getSourceEvent().getPayoutEvent().getId();
            }
        }
        if (event instanceof com.rbkmoney.fistful.wallet.SinkEvent) {
            com.rbkmoney.fistful.wallet.SinkEvent sinkEvent = (com.rbkmoney.fistful.wallet.SinkEvent) event;
            return sinkEvent.getPayload().getId();
        }
        if (event instanceof com.rbkmoney.fistful.identity.SinkEvent) {
            com.rbkmoney.fistful.identity.SinkEvent sinkEvent = (com.rbkmoney.fistful.identity.SinkEvent) event;
            return sinkEvent.getPayload().getId();
        }
        if (event instanceof com.rbkmoney.fistful.withdrawal.SinkEvent) {
            com.rbkmoney.fistful.withdrawal.SinkEvent sinkEvent = (com.rbkmoney.fistful.withdrawal.SinkEvent) event;
            return sinkEvent.getPayload().getId();
        }
        return null;
    }

    public static <T> TemporalAccessor getCreatedAt(T event) {
        if (event == null) {
            return null;
        }
        String time = null;
        if (event instanceof StockEvent) {
            StockEvent stockEvent = (StockEvent) event;
            if (stockEvent.getSourceEvent().isSetProcessingEvent()) {
                time = stockEvent.getSourceEvent().getProcessingEvent().getCreatedAt();
            } else if (stockEvent.getSourceEvent().isSetPayoutEvent()) {
                time = stockEvent.getSourceEvent().getPayoutEvent().getCreatedAt();
            }
        }
        if (event instanceof com.rbkmoney.fistful.wallet.SinkEvent) {
            com.rbkmoney.fistful.wallet.SinkEvent sinkEvent = (com.rbkmoney.fistful.wallet.SinkEvent) event;
            time = sinkEvent.getCreatedAt();
        }
        if (event instanceof com.rbkmoney.fistful.identity.SinkEvent) {
            com.rbkmoney.fistful.identity.SinkEvent sinkEvent = (com.rbkmoney.fistful.identity.SinkEvent) event;
            time = sinkEvent.getCreatedAt();
        }
        if (event instanceof com.rbkmoney.fistful.withdrawal.SinkEvent) {
            com.rbkmoney.fistful.withdrawal.SinkEvent sinkEvent = (com.rbkmoney.fistful.withdrawal.SinkEvent) event;
            time = sinkEvent.getCreatedAt();
        }
        return time != null ? TypeUtil.stringToTemporal(time) : null;
    }
}
