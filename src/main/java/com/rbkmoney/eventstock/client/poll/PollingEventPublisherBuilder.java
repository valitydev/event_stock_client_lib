package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.EventRepositorySrv;
import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.eventstock.client.ErrorActionType;
import com.rbkmoney.eventstock.client.ErrorHandler;
import com.rbkmoney.eventstock.client.EventHandler;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class PollingEventPublisherBuilder {
    private static final EventHandler DEFAULT_EVENT_HANDLER =   new EventHandler() {
        private final Logger log = LoggerFactory.getLogger(this.getClass());

        @Override
        public void handleEvent(Object event, String subsKey) {
            log.trace("Subscription: {}, new event: {}", subsKey, event);
        }

        @Override
        public void handleNoMoreElements(String subsKey) {
            log.debug("Subscription exhausted: {}", subsKey);
        }
    };

    private static final ErrorHandler DEFAULT_ERROR_HANDLER = new ErrorHandler() {
        private  final Logger log = LoggerFactory.getLogger(this.getClass());

        @Override
        public ErrorActionType handleError(String subsKey, Throwable errCause) {
            log.error("Subscription error: " + subsKey, errCause);
            return ErrorActionType.INTERRUPT;
        }
    };

    private static final int DEFAULT_MAX_QUERY_SIZE = 100;
    private static final int DEFAULT_MAX_POOL_SIZE = -1;
    private static final int DEFAULT_MAX_POLL_DELAY = 1000;

    private URI uri;
    private EventHandler eventHandler;
    private ErrorHandler errorHandler;
    private int maxQuerySize = DEFAULT_MAX_QUERY_SIZE;
    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
    private int pollDelay = DEFAULT_MAX_POLL_DELAY;

    public PollingEventPublisherBuilder withURI(URI uri) {
        this.uri = uri;
        return this;
    }

    public EventHandler getEventHandler() {
        return eventHandler;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public int getMaxQuerySize() {
        return maxQuerySize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public int getPollDelay() {
        return pollDelay;
    }

    public PollingEventPublisherBuilder withEventHandler(EventHandler eventHandler) {
        if (eventHandler == null) {
            throw new NullPointerException("Null event handler");
        }
        this.eventHandler = eventHandler;
        return this;
    }

    public PollingEventPublisherBuilder withErrorHandler(ErrorHandler errorHandler) {
        if (errorHandler == null) {
            throw new NullPointerException("Null event handler");
        }
        this.errorHandler = errorHandler;
        return this;
    }

    public PollingEventPublisherBuilder withMaxQuerySize(int maxQuerySize) {
        if (maxQuerySize <= 0) {
            throw new IllegalArgumentException("Max query size must be > 0");
        }
        this.maxQuerySize = maxQuerySize;
        return this;
    }

    public PollingEventPublisherBuilder withMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    public PollingEventPublisherBuilder withPollDelay(int pollingDelayMs) {
        if (pollingDelayMs < 0) {
            throw new IllegalArgumentException("Poll delay must be > 0");
        }
        this.pollDelay = pollingDelayMs;
        return this;
    }



    protected THSpawnClientBuilder getClientBuilder() {
        THSpawnClientBuilder clientBuilder = new THSpawnClientBuilder().withAddress(uri);
        return clientBuilder;
    }

    public PollingEventPublisher<StockEvent> build() {
        THSpawnClientBuilder clientBuilder = getClientBuilder();
        ESServiceAdapter serviceAdapter = new ESServiceAdapter(clientBuilder.build(EventRepositorySrv.Iface.class));
        Poller poller = new Poller(serviceAdapter, maxPoolSize, pollDelay);

        PollingConfig<StockEvent> pollingConfig = new PollingConfig<>(
                eventHandler == null ? DEFAULT_EVENT_HANDLER : eventHandler,
                errorHandler == null ? DEFAULT_ERROR_HANDLER : errorHandler,
                maxQuerySize);

        PollingEventPublisher eventPublisher = new PollingEventPublisher(pollingConfig, poller);
        return eventPublisher;
    }
}
