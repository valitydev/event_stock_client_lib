package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.HandlerListener;
import com.rbkmoney.woody.api.ClientBuilder;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;

import java.net.URI;
import java.util.Objects;

public class FistfulPollingEventPublisherBuilder extends DefaultPollingEventPublisherBuilder {
    private static final int DEFAULT_HOUSEKEEPER_TIMEOUT = 1000;

    private URI uri;

    private ClientBuilder clientBuilder;

    private HandlerListener.EventConsumer eventConsumer;

    private long housekeeperTimeout;

    private ServiceAdapterType serviceAdapterType = ServiceAdapterType.WITHDRAWAL;

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

    public FistfulPollingEventPublisherBuilder withEventConsumer(HandlerListener.EventConsumer eventConsumer) {
        this.eventConsumer = eventConsumer;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withHousekeeperTimeout(long housekeeperTimeout) {
        if (housekeeperTimeout <= 0) {
            throw new IllegalArgumentException("Timeout must be > 0");
        }
        this.housekeeperTimeout = housekeeperTimeout;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withURI(URI uri) {
        this.uri = uri;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withClientBuilder(ClientBuilder clientBuilder) {
        Objects.requireNonNull(clientBuilder, "Null client builder");
        this.clientBuilder = clientBuilder;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withWithdrawalServiceAdapter() {
        this.serviceAdapterType = ServiceAdapterType.WITHDRAWAL;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withWalletServiceAdapter() {
        this.serviceAdapterType = ServiceAdapterType.WALLET;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withIdentityServiceAdapter() {
        this.serviceAdapterType = ServiceAdapterType.IDENTITY;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withDepositServiceAdapter() {
        this.serviceAdapterType = ServiceAdapterType.DEPOSIT;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withSourceServiceAdapter() {
        this.serviceAdapterType = ServiceAdapterType.SOURCE;
        return this;
    }

    public FistfulPollingEventPublisherBuilder withDestinationServiceAdapter() {
        this.serviceAdapterType = ServiceAdapterType.DESTINATION;
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
        return createServiceAdapter(getClientBuilder());
    }

    protected ServiceAdapter createServiceAdapter(ClientBuilder clientBuilder) {
        switch (serviceAdapterType) {
            case WITHDRAWAL:
                return FistfulServiceAdapter.buildWithdrawalAdapter(clientBuilder);
            case WALLET:
                return FistfulServiceAdapter.buildWalletAdapter(clientBuilder);
            case IDENTITY:
                return FistfulServiceAdapter.buildIdentityAdapter(clientBuilder);
            case DEPOSIT:
                return FistfulServiceAdapter.buildDepositAdapter(clientBuilder);
            case SOURCE:
                return FistfulServiceAdapter.buildSourceAdapter(clientBuilder);
            case DESTINATION:
                return FistfulServiceAdapter.buildDestinationAdapter(clientBuilder);
            default:
                throw new IllegalArgumentException("Unknown service adapter type");
        }
    }

    public enum ServiceAdapterType {
        IDENTITY,
        WITHDRAWAL,
        WALLET,
        DEPOSIT,
        SOURCE,
        DESTINATION
    }

}
