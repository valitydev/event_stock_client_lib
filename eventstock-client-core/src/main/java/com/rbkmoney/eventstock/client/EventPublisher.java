package com.rbkmoney.eventstock.client;

public interface EventPublisher<TEvent> {
    String subscribe(SubscriberConfig<TEvent> subscriberConfig);

    boolean unsubscribe(String subsKey);

    void unsubscribeAll();

    void destroy();
}
