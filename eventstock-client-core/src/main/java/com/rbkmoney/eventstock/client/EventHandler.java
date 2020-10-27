package com.rbkmoney.eventstock.client;

public interface EventHandler<EType> {
    EventAction handle(EType event, String subsKey) throws Exception;

    default void handleCompleted(String subsKey) throws Exception {
    }

    default void handleInterrupted(String subsKey) throws Exception {
    }
}
