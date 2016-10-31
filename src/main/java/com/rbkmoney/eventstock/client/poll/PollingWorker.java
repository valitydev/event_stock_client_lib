package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.DatasetTooBig;
import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.woody.api.concurrent.WRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by vpankrashkin on 12.07.16.
 */
class PollingWorker implements Runnable {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Poller poller;
    private final PollingConfig<StockEvent> pollingConfig;
    private final ServiceAdapter<StockEvent, EventConstraint> serviceAdapter;
    private final String subscriptionKey;
    private final Runnable wRunnable;
    private RangeWalker<? extends Comparable, ? extends EventRange> rangeWalker;
    private boolean running = true;
    private int pollingLimit;

    public PollingWorker(Poller poller, PollingConfig<StockEvent> pollingConfig, ServiceAdapter<StockEvent, EventConstraint> serviceAdapter, String subscriptionKey) {
        this.poller = poller;
        this.pollingConfig = pollingConfig;
        this.serviceAdapter = serviceAdapter;
        this.subscriptionKey = subscriptionKey;
        this.wRunnable = WRunnable.create(() -> runPolling());
        this.pollingLimit = pollingConfig.getMaxQuerySize();
    }

    @Override
    public void run() {
        wRunnable.run();
    }

    private synchronized void runPolling() {
        try {
            LogSupport.setSubscriptionKey(subscriptionKey);
            boolean exhausted = false;
            boolean suspended = false;

            try {
                if (rangeWalker == null) {
                    log.debug("Range is not initialized, init range");
                    rangeWalker = initRange(pollingConfig.getEventFilter().getEventConstraint());
                    if (rangeWalker == null) {
                        log.debug("Range walker is not initialized, pause");
                        return;
                    } else {
                        log.debug("Range initialized: {}", rangeWalker);
                    }
                }

                while (running && !exhausted && !suspended && !Thread.currentThread().isInterrupted()) {
                    if (rangeWalker.isRangeOver()) {
                        log.debug("Range is over: {}", rangeWalker);
                        exhausted = true;
                        continue;
                    }

                    EventConstraint currentConstraint = new EventConstraint(rangeWalker.getWalkingRange());

                    log.debug("Tying to get event range, constraint: {}, limit: {}", currentConstraint, pollingLimit);
                    Collection<StockEvent> events = serviceAdapter.getEventRange(currentConstraint, pollingLimit);
                    EventHandler<StockEvent> eventHandler = pollingConfig.getEventHandler();
                    StockEvent lastEvent = null;
                    for (StockEvent event : events) {
                        if (!running) {
                            break;
                        }
                        lastEvent = event;
                        try {
                            if (!pollingConfig.getEventFilter().accept(event)) {
                                log.trace("Event not accepted: {}", event);
                                continue;
                            }
                            log.trace("Event accepted: {}", event);
                            eventHandler.handleEvent(event, subscriptionKey);
                        } catch (Throwable t) {
                            if (markIfInterrupted(t)) {
                                log.error("Event handling was interrupted, [break]");
                                break;
                            } else {
                                log.error("Error during event handling event: [" + event + "]", t);
                            }
                        }
                    }
                    if (events.size() < pollingLimit) {
                        if (rangeWalker.getWalkingRange().isToDefined()) {
                            exhausted = true;
                        } else {
                            suspended = true;
                        }
                    }
                    if (lastEvent != null) {
                        final StockEvent tmpLastEvent = lastEvent;
                        rangeWalker.moveRange((walker, boundInclusive) -> {
                            Comparable val;
                            if (walker instanceof IdRangeWalker) {
                                val = ValuesExtractor.getEventId(tmpLastEvent);
                            } else {
                                val = Instant.from(ValuesExtractor.getCreatedAt(tmpLastEvent));
                            }
                            return new Pair(val, false);
                        });
                        log.debug("Range moved to: {}", rangeWalker);
                    }
                }


            } catch (ServiceException e) {
                if (e.getCause() instanceof DatasetTooBig) {
                    DatasetTooBig dtbEx = (DatasetTooBig) e.getCause();
                    log.info("Current query size: '{}' is too big, new size is: '{}'", pollingLimit, dtbEx.getLimit());
                    //we shouldn't get into DatasetTooBig often so we can afford waiting an iteration to continue. This can be changed later.
                    pollingLimit = dtbEx.getLimit();
                } else if (markIfInterrupted(e.getCause())) {
                    log.info("Task interrupted [break]");
                    return;
                } else  {
                    log.warn("Failed to execute request to repository service, caused by: {}", e.getMessage());

                    try {
                        ErrorActionType actionType = pollingConfig.getErrorHandler().handleError(subscriptionKey, e);
                        switch (actionType) {
                            case RETRY:
                                log.warn("Retry request after error");
                                return;
                            case INTERRUPT:
                                log.warn("Interrupt request after error");
                                exhausted = true;
                                break;
                            default:
                                throw new IllegalStateException("Unknown error action: " + actionType);
                        }
                    } catch (Throwable t) {
                        log.error("Error during error handling", t);
                        markIfInterrupted(t);
                    }
                }
            }

            if (exhausted) {
                log.debug("Subscription exhausted");
                poller.directRemovePolling(subscriptionKey);
                try {
                    pollingConfig.getEventHandler().handleNoMoreElements(subscriptionKey);
                } catch (Throwable t) {
                    log.error("Error during event handling [exhausted]", t);
                    markIfInterrupted(t);
                }
            }
        } catch (Throwable t) {
            log.error("Error during poll processing, task is broken", t);
            if (!markIfInterrupted(t)) {
                throw new RuntimeException("Task is broken", t);
            }
        } finally {
            LogSupport.removeSubscriptionKey();
        }
    }

    void stop() {
        running = false;
    }

    private RangeWalker initRange(EventConstraint constraint) throws ServiceException {
        if (constraint.getIdRange() != null) {
            EventConstraint.EventIDRange idRange = constraint.getIdRange();
            return initRange(idRange, IdRangeWalker::new, ValuesExtractor::getEventId, () -> new EventConstraint.EventIDRange(1L, 0L));
        } else {
            EventConstraint.EventTimeRange timeRange = constraint.getTimeRange();
            return initRange(timeRange, TimeRangeWalker::new, (event) -> Instant.from(ValuesExtractor.getCreatedAt(event)), () -> new EventConstraint.EventTimeRange(Instant.MAX, Instant.MIN));
        }
    }

    private <T extends Comparable, R extends EventRange, RW extends RangeWalker> RW initRange(R range, Function<R, RW> walkerCreator, Function<StockEvent, T> valExtractor, Supplier<R> emptyRangeSupplier) throws ServiceException {
        log.debug("Trying to initialize range base on: {}", range);
        RW rangeWalker;
        if (range.isFromDefined()) {
            rangeWalker = walkerCreator.apply(range);
        } else {
            StockEvent event = range.isFromNow() ? serviceAdapter.getLastEvent() : serviceAdapter.getFirstEvent();
            if (event == null) {
                log.trace("No events in stock");
                rangeWalker = null;
            } else {
                T val = valExtractor.apply(event);
                range.setFromInclusive(val);
                rangeWalker = walkerCreator.apply(range);
            }
        }
        return rangeWalker;
    }


    private static boolean markIfInterrupted(Throwable t) {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return true;
        }
        return false;
    }

}
