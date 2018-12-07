package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.HandlerListener;
import com.rbkmoney.woody.api.ClientBuilder;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;

import java.net.URI;
import java.util.Objects;

public abstract class AbstractPollingEventPublisherBuilder<T extends AbstractPollingEventPublisherBuilder>
        extends DefaultPollingEventPublisherBuilder {

    private static final int DEFAULT_HOUSEKEEPER_TIMEOUT = 1000;

    private long housekeeperTimeout;
    private URI uri;
    private ClientBuilder clientBuilder;
    private HandlerListener.EventConsumer eventConsumer;

    @Override
    protected HandlerListener createHandlerListener() {
        if (eventConsumer != null) {
            return new Housekeeper(eventConsumer, getMaxPoolSize(), getHousekeeperTimeout());
        } else {
            return new Housekeeper(getMaxPoolSize(), getHousekeeperTimeout());
        }
    }

    @Override
    protected ServiceAdapter createServiceAdapter() {
        return createServiceAdapter(createClientBuilder());
    }

    public URI getUri() {
        return uri;
    }

    public long getHousekeeperTimeout() {
        if (housekeeperTimeout <= 0) {
            housekeeperTimeout = DEFAULT_HOUSEKEEPER_TIMEOUT;
        }
        return housekeeperTimeout;
    }

    public HandlerListener.EventConsumer getEventConsumer() {
        return eventConsumer;
    }

    public T withEventConsumer(HandlerListener.EventConsumer eventConsumer) {
        this.eventConsumer = eventConsumer;
        return (T) this;
    }

    public T withHousekeeperTimeout(long housekeeperTimeout) {
        if (housekeeperTimeout <= 0) {
            throw new IllegalArgumentException("Timeout must be > 0");
        }
        this.housekeeperTimeout = housekeeperTimeout;
        return (T) this;
    }

    public T withURI(URI uri) {
        this.uri = uri;
        return (T) this;
    }

    public T withClientBuilder(ClientBuilder clientBuilder) {
        Objects.requireNonNull(clientBuilder, "Null client builder");
        this.clientBuilder = clientBuilder;
        return (T) this;
    }

    protected abstract ServiceAdapter createServiceAdapter(ClientBuilder clientBuilder);

    private ClientBuilder createClientBuilder() {
        if (clientBuilder == null) {
            if (uri != null) {
                clientBuilder = new THSpawnClientBuilder().withAddress(uri);
            } else {
                throw new IllegalArgumentException("Uri not be null");
            }
        }
        return clientBuilder;
    }
}
