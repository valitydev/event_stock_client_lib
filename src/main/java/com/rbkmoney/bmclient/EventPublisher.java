package com.rbkmoney.bmclient;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface EventPublisher<TEvent> {
    String subscribe(EventFilter<TEvent> filter);
    String subscribe(EventFilter<TEvent> filter, EventHandler<TEvent> eventHandler);
    String subscribe(EventFilter<TEvent> filter, EventHandler<TEvent> eventHandler, ErrorHandler errorHandler);
    boolean unsubscribe(String subsKey);
    void unsubscribeAll();
    void destroy();
}
