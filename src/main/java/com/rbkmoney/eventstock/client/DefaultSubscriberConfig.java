package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 11.07.16.
 */
public class DefaultSubscriberConfig<TEvent> implements SubscriberConfig<TEvent> {
    private final EventFilter<TEvent> filter;
    private final EventHandler<TEvent> handler;
    private final ErrorHandler errorHandler;
    private final int maxQuerySize;

    public DefaultSubscriberConfig(EventFilter<TEvent> filter) {
        this(filter, null);
    }

    public DefaultSubscriberConfig(EventFilter<TEvent> filter, EventHandler<TEvent> handler) {
        this(filter, handler, null, -1);
    }

    public DefaultSubscriberConfig(EventFilter<TEvent> filter, EventHandler<TEvent> handler, ErrorHandler errorHandler, int maxQuerySize) {
        this.filter = filter;
        this.handler = handler;
        this.errorHandler = errorHandler;
        this.maxQuerySize = maxQuerySize;
    }

    @Override
    public EventFilter<TEvent> getEventFilter() {
        return filter;
    }

    @Override
    public EventHandler<TEvent> getEventHandler() {
        return handler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public int getMaxQuerySize() {
        return maxQuerySize;
    }
}
