package com.rbkmoney.bmclient.polling;

import com.rbkmoney.bmclient.ErrorActionType;
import com.rbkmoney.bmclient.ErrorHandler;
import com.rbkmoney.bmclient.EventHandler;
import com.rbkmoney.damsel.event_stock.EventRepositorySrv;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class BMEventPublisherBuilder {
    private static final EventHandler DEFAULT_EVENT_HANDLER =   new EventHandler() {
        @Override
        public void handleEvent(Object event, String subsKey) {
        }

        @Override
        public void handleNoMoreElements(String subsKey) {
        }
    };
    private static final ErrorHandler DEFALULT_ERROR_HANDLER = new ErrorHandler() {
        private  final Logger log = LoggerFactory.getLogger(this.getClass());

        @Override
        public ErrorActionType handleError(Object source, Throwable errCause) {
            log.error("Error", errCause);
            return ErrorActionType.INTERRUPT;
        }
    };

    private URI uri;
    private EventHandler eventHandler;
    private ErrorHandler errorHandler;

    public BMEventPublisherBuilder withURI(URI uri) {
        this.uri = uri;
        return this;
    }

    public BMEventPublisherBuilder withEventHandler(EventHandler eventHandler) {
        this.eventHandler = eventHandler;
        return this;
    }

    public BMEventPublisherBuilder withErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }



    public BMPollingEventPublisher build() {
        THSpawnClientBuilder clientBuilder = new THSpawnClientBuilder();
        clientBuilder.withAddress(uri);
        BMServiceAdapter serviceAdapter = new BMServiceAdapter(clientBuilder.build(EventRepositorySrv.Iface.class));
        PollingRunner pollingRunner = new BMPollingRunner(serviceAdapter);

        BMPollingEventPublisher eventPublisher = new BMPollingEventPublisher(eventHandler == null ? DEFAULT_EVENT_HANDLER : eventHandler, errorHandler == null ? DEFALULT_ERROR_HANDLER : errorHandler, pollingRunner);
        return eventPublisher;
    }
}
