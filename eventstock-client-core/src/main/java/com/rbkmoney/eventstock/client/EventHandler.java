package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface EventHandler<EType> {
    EventAction handle(EType event, String subsKey) throws Exception;
    default void handleCompleted(String subsKey) throws Exception {}
    default void handleInterrupted(String subsKey) throws Exception {}
}
