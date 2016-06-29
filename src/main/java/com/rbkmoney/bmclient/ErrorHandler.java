package com.rbkmoney.bmclient;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface ErrorHandler {
    ErrorActionType handleError(Object source, Throwable errCause);
}
