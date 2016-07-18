package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 11.07.16.
 */
public interface SubscriberConfig<TEvent> {
    EventFilter<TEvent> getEventFilter();
    EventHandler<TEvent> getEventHandler();
    ErrorHandler getErrorHandler();
    int getMaxQuerySize();
}
