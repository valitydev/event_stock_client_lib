package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.ErrorHandler;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.eventstock.client.EventHandler;
import com.rbkmoney.eventstock.client.SubscriberConfig;

import java.util.Objects;

class PollingConfig<TEvent> implements SubscriberConfig<TEvent> {
    private final EventFilter<TEvent> eventFilter;
    private final EventHandler<TEvent> eventHandler;
    private final ErrorHandler errorHandler;
    private final int maxQuerySize;
    private final int eventRetryDelay;

    public static <TEvent> PollingConfig<TEvent> mergeConfig(PollingConfig<TEvent> mainConfig, PollingConfig<TEvent> defaultConfig) {
        EventFilter<TEvent> eventFilter = mainConfig.getEventFilter() != null ? mainConfig.getEventFilter() : defaultConfig.getEventFilter();
        EventHandler<TEvent> eventHandler = mainConfig.getEventHandler() != null ? mainConfig.getEventHandler() : defaultConfig.getEventHandler();
        ErrorHandler errorHandler = mainConfig.getErrorHandler() != null ? mainConfig.getErrorHandler() : defaultConfig.getErrorHandler();
        int maxQuerySize = mainConfig.getMaxQuerySize() > 0 ? mainConfig.getMaxQuerySize() : defaultConfig.getMaxQuerySize();
        int eventRetryDelay = mainConfig.getEventRetryDelay() > 0 ? mainConfig.getEventRetryDelay() : defaultConfig.getEventRetryDelay();
        return new PollingConfig<>(eventFilter, eventHandler, errorHandler, maxQuerySize, eventRetryDelay, true);
    }

    public PollingConfig(SubscriberConfig<TEvent> subscriberConfig) {
        this(subscriberConfig.getEventFilter(), subscriberConfig.getEventHandler(), subscriberConfig.getErrorHandler(), subscriberConfig.getMaxQuerySize(), subscriberConfig.getEventRetryDelay(), false);
    }

    public PollingConfig(EventFilter<TEvent> eventFilter, EventHandler<TEvent> eventHandler, ErrorHandler errorHandler, int maxQuerySize, int eventRetryDelay) {
        this(eventFilter, eventHandler, errorHandler, maxQuerySize, eventRetryDelay, false);
    }

    private PollingConfig(EventFilter<TEvent> eventFilter, EventHandler<TEvent> eventHandler, ErrorHandler errorHandler, int maxQuerySize, int eventRetryDelay, boolean strict) {
        if (strict) {
            Objects.requireNonNull(eventFilter, "Filter cannot be null");
            Objects.requireNonNull(eventHandler, "Event handler cannot be null");
            Objects.requireNonNull(errorHandler, "Error handler cannot be null");
        }

        if (eventRetryDelay < 0) {
            throw new IllegalArgumentException("Retry timeout cannot be negative");
        }

        this.eventFilter = eventFilter;
        this.eventHandler = eventHandler;
        this.errorHandler = errorHandler;
        this.maxQuerySize = maxQuerySize;
        this.eventRetryDelay = eventRetryDelay;
    }

    @Override
    public EventFilter<TEvent> getEventFilter() {
        return eventFilter;
    }

    @Override
    public EventHandler<TEvent> getEventHandler() {
        return eventHandler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public int getMaxQuerySize() {
        return maxQuerySize;
    }

    @Override
    public int getEventRetryDelay() {
        return eventRetryDelay;
    }

    @Override
    public String toString() {
        return "PollingConfig{" +
                "eventFilter=" + eventFilter +
                ", eventHandler=" + eventHandler +
                ", errorHandler=" + errorHandler +
                ", maxQuerySize=" + maxQuerySize +
                ", eventRetryDelay=" + eventRetryDelay +
                '}';
    }
}
