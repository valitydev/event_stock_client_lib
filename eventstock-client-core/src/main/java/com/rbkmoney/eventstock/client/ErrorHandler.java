package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface ErrorHandler {
    ErrorAction handleError(String subsKey, Throwable errCause);
}
