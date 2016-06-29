package com.rbkmoney.bmclient;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface EventFilter<TEvent> {
    boolean accept(TEvent event);
}
