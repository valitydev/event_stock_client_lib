package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Created by vpankrashkin on 28.06.16.
 */
class PollingEventPublisher<TEvent> implements EventPublisher<TEvent> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final PollingConfig defaultConfig;
    private final Poller poller;

    public PollingEventPublisher(PollingConfig defaultConfig, Poller poller) {
        this.defaultConfig = defaultConfig;
        this.poller = poller;
    }

    @Override
    public String subscribe(SubscriberConfig<TEvent> subscriberConfig) {
        PollingConfig<TEvent> mainConfig;
        if (subscriberConfig instanceof PollingConfig) {
            mainConfig = (PollingConfig<TEvent>) subscriberConfig;
        } else {
            mainConfig = new PollingConfig<>(subscriberConfig);
        }

        PollingConfig resultConfig = PollingConfig.mergeConfig(mainConfig, defaultConfig);
        log.trace("Merged PollingConfig: {}", resultConfig);
        do {
            String subsKey = UUID.randomUUID().toString();
            if (poller.addPolling(subsKey, resultConfig)) {
                log.debug("Successfully subscribed: {}", subsKey);
                return subsKey;
            }
        } while (!Thread.currentThread().isInterrupted());
        log.warn("Subscription interrupted, no subscription key generated");
        return null;
    }

    @Override
    public boolean unsubscribe(String subsKey) {
        return poller.removePolling(subsKey);
    }

    @Override
    public void unsubscribeAll() {
        poller.removeAll();
    }

    @Override
    public void destroy() {
        unsubscribeAll();
        poller.destroy();
    }

}
