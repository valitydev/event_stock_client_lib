package com.rbkmoney.bmclient.polling;

import com.rbkmoney.bmclient.ErrorHandler;
import com.rbkmoney.bmclient.EventHandler;
import com.rbkmoney.damsel.event_stock.StockEvent;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class BMPollingEventPublisher extends AbstractPollingEventPublisher<StockEvent> {
    public BMPollingEventPublisher(EventHandler<StockEvent> defaultEventHandler, ErrorHandler defaultErrorHandler, PollingRunner pollingRunner) {
        super(defaultEventHandler, defaultErrorHandler, pollingRunner);
    }
}
