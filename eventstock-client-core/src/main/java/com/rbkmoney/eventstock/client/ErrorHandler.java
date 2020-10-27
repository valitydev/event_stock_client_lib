package com.rbkmoney.eventstock.client;

public interface ErrorHandler {
    ErrorAction handleError(String subsKey, Throwable errCause);
}
