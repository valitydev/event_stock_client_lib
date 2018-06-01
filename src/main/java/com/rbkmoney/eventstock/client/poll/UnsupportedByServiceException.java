package com.rbkmoney.eventstock.client.poll;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public class UnsupportedByServiceException extends ServiceException {
    public UnsupportedByServiceException() {
        super();
    }

    public UnsupportedByServiceException(String message) {
        super(message);
    }

    public UnsupportedByServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportedByServiceException(Throwable cause) {
        super(cause);
    }

    protected UnsupportedByServiceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
