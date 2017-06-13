package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface EventHandler<EType> {
    EventAction handle(EType event, String subsKey);
    default void handleCompleted(String subsKey) {}
    default void handleInterrupted(String subsKey) {}
}
