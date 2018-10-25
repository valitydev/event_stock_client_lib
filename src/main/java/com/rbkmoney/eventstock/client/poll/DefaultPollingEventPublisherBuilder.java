package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.eventstock.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class DefaultPollingEventPublisherBuilder {
    protected static final EventHandler DEFAULT_EVENT_HANDLER = new EventHandler() {
        private final Logger log = LoggerFactory.getLogger(this.getClass());

        @Override
        public EventAction handle(Object event, String subsKey) {
            log.trace("Subscription: {}, new event: {}", subsKey, event);
            return EventAction.CONTINUE;
        }

        @Override
        public void handleCompleted(String subsKey) {
            log.debug("Subscription completed: {}", subsKey);
        }

        @Override
        public void handleInterrupted(String subsKey) {
            log.debug("Subscription interrupted: {}", subsKey);
        }
    };

    protected static final ErrorHandler DEFAULT_ERROR_HANDLER = new ErrorHandler() {
        private  final Logger log = LoggerFactory.getLogger(this.getClass());

        @Override
        public ErrorAction handleError(String subsKey, Throwable errCause) {
            log.warn("Subscription error, retry: " + subsKey, errCause);
            return ErrorAction.RETRY;
        }
    };

    private static final HandlerListener DEFAULT_HANDLER_LISTENER = new HandlerListener() {
        @Override
        public void beforeHandle(int bindingId, Object event, String subsKey) {}

        @Override
        public void afterHandle(int bindingId, Object event, String subsKey) {}

        @Override
        public int bindId(Thread worker) {
            return 0;
        }

        @Override
        public void unbindId(Thread worker) {}

        @Override
        public void destroy() {}
    };
    protected static final int DEFAULT_MAX_QUERY_SIZE = 100;
    protected static final int DEFAULT_MAX_POOL_SIZE = 1;

    protected static final int DEFAULT_MAX_POLL_DELAY = 1000;
    protected static final int DEFAULT_EVENT_RETRY_DELAY = 1000;

    private EventHandler eventHandler;
    private ErrorHandler errorHandler;
    private ServiceAdapter serviceAdapter;
    private HandlerListener handlerListener;
    private int maxQuerySize = DEFAULT_MAX_QUERY_SIZE;
    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
    private int pollDelay = DEFAULT_MAX_POLL_DELAY;
    private int eventRetryDelay = DEFAULT_EVENT_RETRY_DELAY;

    public HandlerListener getHandlerListener() {
        if (handlerListener == null) {
            handlerListener = createHandlerListener();
        }
        return handlerListener;
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

    public int getEventRetryDelay() {
        return eventRetryDelay;
    }

    public DefaultPollingEventPublisherBuilder withHandlerListener(HandlerListener handlerListener) {
        Objects.requireNonNull(handlerListener, "Null handler listener");
        this.handlerListener = handlerListener;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withEventHandler(EventHandler eventHandler) {
        Objects.requireNonNull(eventHandler, "Null event handler");
        this.eventHandler = eventHandler;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withErrorHandler(ErrorHandler errorHandler) {
        Objects.requireNonNull(errorHandler, "Null error event handler");
        this.errorHandler = errorHandler;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withMaxQuerySize(int maxQuerySize) {
        if (maxQuerySize <= 0) {
            throw new IllegalArgumentException("Max query size must be > 0");
        }
        this.maxQuerySize = maxQuerySize;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withPollDelay(int pollingDelayMs) {
        if (pollingDelayMs < 0) {
            throw new IllegalArgumentException("Poll delay must be >= 0");
        }
        this.pollDelay = pollingDelayMs;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withEventRetryDelay(int eventRetryDelayMs) {
        if (eventRetryDelayMs < 0) {
            throw new IllegalArgumentException("Event retry delay must be >= 0");
        }
        this.eventRetryDelay = eventRetryDelayMs;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withServiceAdapter(ServiceAdapter serviceAdapter) {
        Objects.requireNonNull(serviceAdapter, "ServiceAdapter cannot be null");
        this.serviceAdapter = serviceAdapter;
        return this;
    }

    public ServiceAdapter getServiceAdapter() {
        if (serviceAdapter == null) {
            serviceAdapter = createServiceAdapter();
        }
        return serviceAdapter;
    }

    protected HandlerListener createHandlerListener() {
        return DEFAULT_HANDLER_LISTENER;
    }

    protected ServiceAdapter createServiceAdapter() {
        throw new UnsupportedOperationException("Cannot create ServiceAdapter");
    }

    public <T> PollingEventPublisher<T> build() {
        ServiceAdapter adapter = getServiceAdapter();
        Poller poller = new Poller(adapter, getHandlerListener(), getMaxPoolSize(), getPollDelay());
        EventHandler eventHandler = getEventHandler();
        eventHandler = eventHandler == null ? DEFAULT_EVENT_HANDLER : eventHandler;
        ErrorHandler errorHandler = getErrorHandler();
        errorHandler = errorHandler == null ? DEFAULT_ERROR_HANDLER : errorHandler;
        PollingConfig<T> pollingConfig = new PollingConfig<>(null, eventHandler, errorHandler, getMaxQuerySize(), getEventRetryDelay());
        PollingEventPublisher eventPublisher = new PollingEventPublisher(pollingConfig, poller);
        return eventPublisher;
    }
}
