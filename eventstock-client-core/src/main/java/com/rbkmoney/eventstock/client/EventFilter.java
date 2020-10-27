package com.rbkmoney.eventstock.client;

import com.rbkmoney.geck.filter.Filter;

import java.time.temporal.TemporalAccessor;

public interface EventFilter<TEvent> {
    EventConstraint getEventConstraint();

    Filter getFilter();

    int getLimit();

    boolean accept(Long id, TemporalAccessor time, TEvent event);
}
