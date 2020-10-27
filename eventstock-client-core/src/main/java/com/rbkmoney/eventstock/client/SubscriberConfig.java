package com.rbkmoney.eventstock.client;

public interface SubscriberConfig<TEvent> {
    EventFilter<TEvent> getEventFilter();

    EventHandler<TEvent> getEventHandler();

    ErrorHandler getErrorHandler();

    int getMaxQuerySize();

    int getEventRetryDelay();
}
