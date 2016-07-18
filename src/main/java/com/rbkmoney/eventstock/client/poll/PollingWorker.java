package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.DatasetTooBig;
import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.eventstock.client.*;
import javafx.util.Pair;
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
    private RangeWalker<? extends Comparable, ? extends EventRange> rangeWalker;
    private boolean running = true;
    private int pollingLimit;

    public PollingWorker(Poller poller, PollingConfig<StockEvent> pollingConfig, ServiceAdapter<StockEvent, EventConstraint> serviceAdapter, String subscriptionKey) {
        this.poller = poller;
        this.pollingConfig = pollingConfig;
        this.serviceAdapter = serviceAdapter;
        this.subscriptionKey = subscriptionKey;
        this.pollingLimit = pollingConfig.getMaxQuerySize();
    }

    @Override
    public synchronized void run() {
        try {
            boolean exhausted = false;
            boolean suspended = false;

            try {
                if (rangeWalker == null) {
                    rangeWalker = initRange(pollingConfig.getEventFilter().getEventConstraint());
                    if (rangeWalker == null) {
                        log.debug("Range walker is not initialized, pause");
                        return;
                    }
                }

                while (!exhausted && !suspended && running && !Thread.currentThread().isInterrupted()) {
                    if (rangeWalker.isRangeOver()) {
                        log.debug("Range is over: {}", rangeWalker);
                        exhausted = true;
                        continue;
                    }

                    EventConstraint currentConstraint = new EventConstraint(rangeWalker.getWalkingRange());

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
                            log.error("Error during event handling [subsKey: " + subscriptionKey + ", event: " + event + "]", t);
                            markIfInterrupted(t);
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
                    }
                }


            } catch (ServiceException e) {
                if (e.getCause() instanceof DatasetTooBig) {
                    DatasetTooBig dtbEx = (DatasetTooBig) e.getCause();
                    log.info("Current query size: '{}' is too big, new size is: '{}'", pollingLimit, dtbEx.getLimit());
                    //we shouldn't get into DatasetTooBig often so we can afford waiting an iteration to continue. This can be changed later.
                    pollingLimit = dtbEx.getLimit();
                } else if (e.getCause() instanceof InterruptedException) {
                    log.info("Task interrupted [break, subsKey: {}]", subscriptionKey);
                    markIfInterrupted(e.getCause());
                    return;
                } else  {
                    log.error("Failed to execute request to repository service", e);
                    ErrorActionType actionType = null;
                    try {
                        actionType = pollingConfig.getErrorHandler().handleError(subscriptionKey, e);
                    } catch (Throwable t) {
                        log.error("Error during error handling [subsKey: " + subscriptionKey + "]", t);
                        markIfInterrupted(t);
                    }
                    switch (actionType) {
                        case RETRY:
                            log.warn("Retry request after error [subsKey: {}]", subscriptionKey);
                            return;
                        case INTERRUPT:
                            log.warn("Interrupt request after error [subsKey: {}]", subscriptionKey);
                            exhausted = true;
                            break;
                        default:
                            throw new IllegalStateException("Unknown error action: " + actionType);
                    }
                }
            }

            if (exhausted) {
                log.debug("Subscription exhausted: {}", subscriptionKey);
                poller.directRemovePolling(subscriptionKey);
                try {
                    pollingConfig.getEventHandler().handleNoMoreElements(subscriptionKey);
                } catch (Throwable t) {
                    log.error("Error during event handling [exhausted, subsKey: " + subscriptionKey + "]", t);
                    markIfInterrupted(t);
                }
            }
        } catch (Throwable t) {
            log.error("Error during poll processing, task is broken", t);
            markIfInterrupted(t);
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
        RW rangeWalker;
        if (range.isFromDefined()) {
            rangeWalker = walkerCreator.apply(range);
        } else {
            StockEvent event = range.isFromNow() ? serviceAdapter.getLastEvent() : serviceAdapter.getFirstEvent();
            if (event == null) {
                log.info("No events in stock");
                if (range.isFromNow()) {
                    rangeWalker = null;
                } else {
                    rangeWalker = walkerCreator.apply(emptyRangeSupplier.get());
                }
            } else {
                T val = valExtractor.apply(event);
                range.setFromValue(val);
                rangeWalker = walkerCreator.apply(range);
            }
        }
        return rangeWalker;
    }


    private static void markIfInterrupted(Throwable t) {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

}
