package com.rbkmoney.eventstock.client.poll;


import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.eventstock.client.EventConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by vpankrashkin on 28.06.16.
 */
class Poller {
    private static final int DEFAULT_MAX_POOL_SIZE = -1;
    private static final int DEFAULT_MAX_POLL_DELAY = 1000;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Map<String, Pair<Future, PollingWorker>> pollers = new HashMap<>();
    private final ServiceAdapter<StockEvent, EventConstraint> serviceAdapter;
    private final Lock lock = new ReentrantLock();
    private final ScheduledThreadPoolExecutor executorService;
    private final int pollDelay;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public Poller(ServiceAdapter<StockEvent, EventConstraint> serviceAdapter) {
        this(serviceAdapter, DEFAULT_MAX_POOL_SIZE, DEFAULT_MAX_POLL_DELAY);
    }

    public Poller(ServiceAdapter<StockEvent, EventConstraint> serviceAdapter, int maxPoolSize, int pollDelay) {
        this.executorService = new ScheduledThreadPoolExecutor(1,
                new ThreadFactory() {
                    AtomicInteger counter = new AtomicInteger();
                    ThreadGroup threadGroup = new ThreadGroup("ESCPollerPool");

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t =  new Thread(threadGroup, r, "T" + counter.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                });
        executorService.setRemoveOnCancelPolicy(true);
        if (maxPoolSize > 0) {
            executorService.setMaximumPoolSize(maxPoolSize);
        }
        this.serviceAdapter = serviceAdapter;
        this.pollDelay = pollDelay;
    }

    boolean addPolling(String subsKey, PollingConfig pollingConfig) {
        checkState();
        lock.lock();
        try {
            checkState();
            if (pollers.containsKey(subsKey)) {
                return false;
            }
            PollingWorker pollingWorker = new PollingWorker(this, pollingConfig, serviceAdapter, subsKey);
            Future future = executorService.scheduleWithFixedDelay(pollingWorker, 0, pollDelay, TimeUnit.MILLISECONDS);
            pollers.put(subsKey, new Pair<>(future, pollingWorker));
            log.debug("Task added: {}", subsKey);
            return true;
        } finally {
            lock.unlock();
        }
    }

    boolean removePolling(String subsKey) {
        checkState();
        lock.lock();
        try {
            checkState();
            return directRemovePolling(subsKey);
        } finally {
            lock.unlock();
        }
    }

    boolean directRemovePolling(String subsKey) {
        lock.lock();
        try {
            boolean removed;
            Pair<Future, PollingWorker> pair = pollers.get(subsKey);
            if (pair != null) {
                pair.getValue().stop();
                pair.getKey().cancel(true);
                pollers.remove(subsKey);
                removed = true;
            } else {
                removed = false;
            }
            logTaskRemoval(removed, subsKey);
            return removed;
        } finally {
            lock.unlock();
        }
    }

    void removeAll() {
        checkState();
        lock.lock();
        try {
            checkState();
            directRemoveAll();
        } finally {
            lock.unlock();
        }
    }

    void directRemoveAll() {
        lock.lock();
        try {
            for (String subsKey : pollers.keySet()) {
                logTaskRemoval(directRemovePolling(subsKey), subsKey);
            }
        } finally {
            lock.unlock();
        }
    }

    void destroy() {
        if (running.compareAndSet(true, false)) {
            log.info("Shutdown poller...");
            lock.lock();
            try {
                directRemoveAll();
            } finally {
                lock.unlock();
            }
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("Failed to stop poller in time.");
                } else {
                    log.info("Poller stopped.");
                }
            } catch (InterruptedException e) {
                log.warn("Waiting for poller shutdown is interrupted.");
            }
        } else {
            log.warn("Poller is already marked as destroyed.");
        }
    }

    private void checkState() {
        if (!running.get()) {
            throw new IllegalStateException("Poller is already destroyed");
        }
    }

    private void logTaskRemoval(boolean succeed, String subsKey) {
        if (succeed) {
            log.debug("Task removed: {}", subsKey);
        } else {
            //log.warn("Task not removed: {}", subsKey);
        }
    }


}
