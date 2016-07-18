package com.rbkmoney.eventstock.client;

import com.rbkmoney.thrift.filter.Filter;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface EventFilter<TEvent> {
    EventConstraint getEventConstraint();
    Filter getFilter();
    int getLimit();
    boolean accept(TEvent event);
}
