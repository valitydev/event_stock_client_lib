package com.rbkmoney.bmclient.polling;

import com.rbkmoney.bmclient.EventHandler;
import com.rbkmoney.damsel.event_stock.DatasetTooBig;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.event_stock.StockEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public class BMPollingRunner implements PollingRunner {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ServiceAdapter<StockEvent, EventRange> serviceAdapter;
    private ExecutorService executor = Executors.newCachedThreadPool();

    private Map<String, BMPollingWorker> pollers = new HashMap<>();

    public BMPollingRunner(ServiceAdapter<StockEvent, EventRange> serviceAdapter) {
        this.serviceAdapter = serviceAdapter;
    }

    @Override
    public boolean addPolling(String subsKey, SubscriberInfo subscriberInfo, int maxBlockSize) {
        if (pollers.containsKey(subsKey)) {
            return false;
        }
        BMPollingWorker pollingWorker = new BMPollingWorker(serviceAdapter, subscriberInfo, subsKey, maxBlockSize);
        executor.submit(pollingWorker);
        pollers.put(subsKey, pollingWorker);
        return true;
    }

    @Override
    public boolean removePolling(String subsKey) {
        BMPollingWorker pollingWorker = pollers.get(subsKey);
        if (pollingWorker != null) {
            pollingWorker.stop();
            //TODO waitnig for worker destruction
        }
        return true;
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
        try {
            if (executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.error("Failed to stop executor in time");
            }
        } catch (InterruptedException e) {
            log.info("Awaiting for executor shutdown is interrupted");
        }
    }

    private static class BMPollingWorker implements Runnable {
        private final Logger log = LoggerFactory.getLogger(this.getClass());

        private final ServiceAdapter<StockEvent, EventRange> serviceAdapter;
        private final SubscriberInfo subscriberInfo;
        private final String subsKey;
        private final int blockSize;
        private final BMRangeChecker rangeChecker = new BMRangeChecker();
        private volatile boolean runFalg = true;



        public BMPollingWorker(ServiceAdapter<StockEvent, EventRange> serviceAdapter, SubscriberInfo subscriberInfo, String subsKey, int blockSize) {
            this.serviceAdapter = serviceAdapter;
            this.subscriberInfo = subscriberInfo;
            this.subsKey = subsKey;
            this.blockSize = blockSize;
        }

        @Override
        public void run() {
            EventRange eventRange = ((BMEventFilter) subscriberInfo.getEventFilter()).getEventRange();
            EventRange currentRange = eventRange;
            int currentBlockSize = blockSize;
            boolean notExhausted = true;
            mailLoop:
            while (runFalg && !Thread.currentThread().isInterrupted() && notExhausted) {
                Collection<StockEvent> events = null;
                try {
                    events = serviceAdapter.getEventRange(currentRange, currentBlockSize);
                    EventHandler eventHandler = subscriberInfo.getEventHandler();
                    StockEvent lastEvent = null;
                    for (StockEvent event: events) {
                        if (!runFalg) {
                            continue mailLoop;
                        }
                        if (!((BMEventFilter) subscriberInfo.getEventFilter()).accept(event)) {
                            log.trace("Event skipped for subscription:{}, event: {}", subsKey, event);
                            continue;
                        }
                        try {
                            lastEvent = event;
                            eventHandler.handleEvent(event, subsKey);
                        } catch (Exception e) {
                            log.error("Event handled with error for subscription:"+subsKey+ "event:"+event, e);
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    if (events.size() < currentBlockSize) {
                        log.debug("Subscription exhausted {}", subsKey);
                        notExhausted = false;
                        subscriberInfo.getEventHandler().handleNoMoreElements(subsKey);
                    } else if (lastEvent != null) {
                        currentRange = increaseIdRange(currentRange, currentBlockSize);
                    }
                } catch (ServiceException e) {
                    if (e.getCause() instanceof DatasetTooBig) {
                        DatasetTooBig dtbEx = (DatasetTooBig) e.getCause();
                        log.info("Current data block size: '{}' is too big, new size is: '{}'", currentBlockSize, dtbEx.getLimit());
                        currentBlockSize = dtbEx.getLimit();
                    }
                    log.error("Failed to execute request to repository service", e);
                }
            }

        }

        private EventRange increaseIdRange(EventRange oldRange, int idStep) {
            long fromId = (Long) oldRange.getIdRange().getFromId().getFieldValue();
            long toId = (Long) oldRange.getIdRange().getToId().getFieldValue();
            EventRange newRange = new EventRange(oldRange);
            long newToId = fromId + idStep;
            newToId = newToId > toId ? toId : newToId;
            if (oldRange.getIdRange().getFromId().isSetInclusive()) {
                newRange.getIdRange().getFromId().setInclusive(newToId);
            } else {
                newRange.getIdRange().getFromId().setExclusive(newToId);
            }
            return newRange;
        }

        public void stop() {
            runFalg = false;
        }
    }
}
