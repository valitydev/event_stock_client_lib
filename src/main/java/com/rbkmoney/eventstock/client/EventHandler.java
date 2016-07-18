package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface EventHandler<EType> {
    void handleEvent(EType event, String subsKey);
    void handleNoMoreElements(String subsKey);
}
