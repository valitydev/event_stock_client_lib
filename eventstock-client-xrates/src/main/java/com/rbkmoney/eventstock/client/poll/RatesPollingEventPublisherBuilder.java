package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.woody.api.ClientBuilder;

public class RatesPollingEventPublisherBuilder extends AbstractPollingEventPublisherBuilder<RatesPollingEventPublisherBuilder> {

    @Override
    protected ServiceAdapter createServiceAdapter(ClientBuilder clientBuilder) {
        return RatesServiceAdapter.build(clientBuilder);
    }
}
