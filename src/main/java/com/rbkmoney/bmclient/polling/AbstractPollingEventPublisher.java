package com.rbkmoney.bmclient.polling;

import com.rbkmoney.bmclient.ErrorHandler;
import com.rbkmoney.bmclient.EventFilter;
import com.rbkmoney.bmclient.EventHandler;
import com.rbkmoney.bmclient.EventPublisher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public class AbstractPollingEventPublisher<TEvent> implements EventPublisher<TEvent> {
    private final HashMap<String, SubscriberInfo> subscribers = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private final EventHandler<TEvent> defaultEventHandler;
    private final ErrorHandler defaultErrorHandler;
    private final PollingRunner pollingRunner;

    private final int defaultBlockSize = 2;

    public AbstractPollingEventPublisher(EventHandler<TEvent> defaultEventHandler, ErrorHandler defaultErrorHandler, PollingRunner pollingRunner) {
        this.defaultEventHandler = defaultEventHandler;
        this.defaultErrorHandler = defaultErrorHandler;
        this.pollingRunner = pollingRunner;
    }

    @Override
    public String subscribe(EventFilter<TEvent> filter) {
        return subscribe(filter, defaultEventHandler);
    }

    @Override
    public String subscribe(EventFilter<TEvent> filter, EventHandler<TEvent> eventHandler) {
        return subscribe(filter, eventHandler, defaultErrorHandler);
    }

    @Override
    public String subscribe(EventFilter<TEvent> filter, EventHandler<TEvent> eventHandler, ErrorHandler errorHandler) {
        SubscriberInfo subscriberInfo = new SubscriberInfo(filter, eventHandler, errorHandler);

        String subsKey;
        do {
            subsKey = UUID.randomUUID().toString();
            lock.lock();
            try {
                if (subscribers.containsKey(subsKey)) {
                    continue;
                } else {
                    subscribers.put(subsKey, subscriberInfo);
                    pollingRunner.addPolling(subsKey, subscriberInfo, defaultBlockSize);
                    break;
                }
            } finally {
                lock.unlock();
            }
        } while (true);
        return subsKey;
    }

    @Override
    public boolean unsubscribe(String subsKey) {
        lock.lock();
        try {
            subscribers.remove(subsKey);
            return pollingRunner.removePolling(subsKey);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unsubscribeAll() {
        lock.lock();
        try {
            for (Iterator<String> it = subscribers.keySet().iterator(); it.hasNext();) {
                String subsKey = it.next();
                it.remove();
                pollingRunner.removePolling(subsKey);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void destroy() {
        lock.lock();
        try {
            unsubscribeAll();
            pollingRunner.destroy();
        } finally {
            lock.unlock();
        }
    }
}
