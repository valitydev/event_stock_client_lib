package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.thrift.filter.Filter;

import java.time.Instant;

/**
 * Created by vpankrashkin on 28.06.16.
 */
class EventFlowFilter implements EventFilter<StockEvent> {
    private final EventConstraint eventConstraint;
    private final Filter filter;
    private final int limit;

    public EventFlowFilter(EventConstraint eventConstraint) {
        this(eventConstraint, null, -1);
    }

    public EventFlowFilter(EventConstraint eventConstraint, Filter filter) {
        this(eventConstraint, filter, -1);
    }

    public EventFlowFilter(EventConstraint eventConstraint, Filter filter, int limit) {
        if (eventConstraint == null) {
            throw new NullPointerException("Constraint is null");
        }
        this.eventConstraint = eventConstraint;
        this.filter = filter;
        this.limit = limit;
    }

    @Override
    public boolean accept(StockEvent stockEvent) {
        Long id = ValuesExtractor.getEventId(stockEvent);
        Instant time = Instant.from(ValuesExtractor.getCreatedAt(stockEvent));

        if (eventConstraint.accept(id, time)) {
            if (filter == null || filter.match(stockEvent.getSourceEvent().getProcessingEvent())) {
                return true;
            }
        }
        return false;
    }

    public EventConstraint getEventConstraint() {
        return eventConstraint;
    }

    public Filter getFilter() {
        return filter;
    }

    @Override
    public int getLimit() {
        return limit;
    }
}
