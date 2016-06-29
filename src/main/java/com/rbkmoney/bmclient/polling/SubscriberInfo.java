package com.rbkmoney.bmclient.polling;

import com.rbkmoney.bmclient.ErrorHandler;
import com.rbkmoney.bmclient.EventFilter;
import com.rbkmoney.bmclient.EventHandler;

/**
 * Created by vpankrashkin on 28.06.16.
 */
class SubscriberInfo {
    private final EventFilter eventFilter;
    private final EventHandler eventHandler;
    private final ErrorHandler errorHandler;

    public SubscriberInfo(EventFilter eventFilter, EventHandler eventHandler, ErrorHandler errorHandler) {
        this.eventFilter = eventFilter;
        this.eventHandler = eventHandler;
        this.errorHandler = errorHandler;
    }

    public EventFilter getEventFilter() {
        return eventFilter;
    }

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }
}
