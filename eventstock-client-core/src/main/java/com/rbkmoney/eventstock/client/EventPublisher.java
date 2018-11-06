package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface EventPublisher<TEvent> {
    String subscribe(SubscriberConfig<TEvent> subscriberConfig);
    boolean unsubscribe(String subsKey);
    void unsubscribeAll();
    void destroy();
}
