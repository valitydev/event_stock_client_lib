package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.geck.filter.Filter;

import java.time.Instant;
import java.time.temporal.TemporalAccessor;

public class EventFlowFilter<TEvent> implements EventFilter<TEvent> {
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
    public boolean accept(Long id, TemporalAccessor time, TEvent event) {
        return eventConstraint.accept(id, Instant.from(time)) && (filter == null || filter.match(event));
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
