package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.EventRepositorySrv;
import com.rbkmoney.eventstock.client.HandlerListener;
import com.rbkmoney.woody.api.ClientBuilder;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;

import java.net.URI;
import java.util.Objects;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class PollingEventPublisherBuilder extends DefaultPollingEventPublisherBuilder {
    private static final int DEFAULT_HOUSEKEEPER_TIMEOUT = 1000;

    private URI uri;

    private ClientBuilder clientBuilder;

    private HandlerListener.EventConsumer eventConsumer;

    private long housekeeperTimeout;

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

    public PollingEventPublisherBuilder withEventConsumer(HandlerListener.EventConsumer eventConsumer) {
        this.eventConsumer = eventConsumer;
        return this;
    }

    public PollingEventPublisherBuilder withHousekeeperTimeout(long housekeeperTimeout) {
        if (housekeeperTimeout <= 0) {
            throw new IllegalArgumentException("Timeout must be > 0");
        }
        this.housekeeperTimeout = housekeeperTimeout;
        return this;
    }

    public PollingEventPublisherBuilder withURI(URI uri) {
        this.uri = uri;
        return this;
    }

    public DefaultPollingEventPublisherBuilder withClientBuilder(ClientBuilder clientBuilder) {
        Objects.requireNonNull(clientBuilder, "Null client builder");
        this.clientBuilder = clientBuilder;
        return this;
    }

    protected ClientBuilder getClientBuilder() {
        if (clientBuilder == null) {
            clientBuilder = new THSpawnClientBuilder().withAddress(uri);
        }
        return clientBuilder;
    }

    @Override
    protected HandlerListener createHandlerListener() {
        if (eventConsumer != null) {
            return new Housekeeper(eventConsumer, getMaxPoolSize(), getHousekeeperTimeout());
        } else {
            return new Housekeeper(getMaxPoolSize(), getHousekeeperTimeout());
        }
    }

    protected ServiceAdapter createServiceAdapter() {
        ClientBuilder clientBuilder = getClientBuilder();
        return new ESServiceAdapter(clientBuilder.build(EventRepositorySrv.Iface.class));
    }

}
