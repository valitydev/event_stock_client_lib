package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.DatasetTooBig;
import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.woody.api.flow.WFlow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by vpankrashkin on 12.07.16.
 */
class PollingWorker<TEvent> implements Runnable {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final int WORKING = 0;
    private static final int SUSPEND = 1;
    private static final int RANGE_OVER = 2;
    private static final int HANDLER_INTERRUPTION = 3;

    private final Poller poller;
    private final PollingConfig<TEvent> pollingConfig;
    private final ServiceAdapter<TEvent, EventConstraint> serviceAdapter;
    private final String subscriptionKey;
    private final Runnable task = () -> runPolling();
    private final WFlow wFlow = new WFlow();
    private final HandlerListener<TEvent> handlerListener;
    private RangeWalker<? extends Comparable, ? extends EventRange> rangeWalker;
    private boolean running = true;
    private boolean noEventsInit = false;
    private int pollingLimit;

    public PollingWorker(Poller poller, PollingConfig<TEvent> pollingConfig, ServiceAdapter<TEvent, EventConstraint> serviceAdapter, HandlerListener<TEvent> handlerListener, String subscriptionKey) {
        Objects.requireNonNull(poller, "Poller required");
        this.poller = poller;
        Objects.requireNonNull(pollingConfig, "PollingConfig required");
        this.pollingConfig = pollingConfig;
        Objects.requireNonNull(poller, "ServiceAdapter required");
        this.serviceAdapter = serviceAdapter;
        Objects.requireNonNull(poller, "HandlerListener required");
        this.handlerListener = handlerListener;
        Objects.requireNonNull(poller, "SubscriptionKey required");
        this.subscriptionKey = subscriptionKey;
        this.pollingLimit = pollingConfig.getMaxQuerySize();
        if (pollingLimit <= 0) {
            throw new IllegalArgumentException("Polling limit must be > 0");
        }
    }

    @Override
    public void run() {
        task.run();
    }

    public PollingConfig<TEvent> getPollingConfig() {
        return pollingConfig;
    }

    private boolean isWorking(Thread worker) {
        return running && !worker.isInterrupted();
    }

    private boolean hasWorkingFlag(int flag) {
        return flag == WORKING;
    }

    private synchronized void runPolling() {
        wFlow.createServiceFork(() -> {
            try {
                LogSupport.setSubscriptionKey(subscriptionKey);
                int completionFlag = WORKING;

                Thread worker = Thread.currentThread();
                int bindingId = handlerListener.bindId(worker);

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

                    while (hasWorkingFlag(completionFlag) && isWorking(worker)) {
                        if (rangeWalker.isRangeOver()) {
                            log.debug("Range is over: {}", rangeWalker);
                            completionFlag = RANGE_OVER;
                            continue;
                        }

                        EventConstraint currentConstraint = new EventConstraint(rangeWalker.getWalkingRange());

                        log.debug("Tying to get event range, constraint: {}, limit: {}", currentConstraint, pollingLimit);
                        Collection<TEvent> events = serviceAdapter.getEventRange(currentConstraint, pollingLimit);

                        TEvent event = null;
                        try {
                            for (Iterator<TEvent> it = events.iterator(); hasWorkingFlag(completionFlag) && it.hasNext(); ) {
                                event = it.next();
                                try {
                                    if (!pollingConfig.getEventFilter().accept(event)) {
                                        log.trace("Event not accepted: {}", event);
                                        continue;
                                    }
                                    log.trace("Event accepted: {}", event);

                                    completionFlag = processEvent(event, bindingId, worker);
                                } catch (Throwable t) {
                                    if (markIfInterrupted(t)) {
                                        log.error("Event handling was interrupted, [break]");
                                        break;
                                    } else {
                                        log.warn("Error during handling event: [" + event + "]", t);
                                    }
                                }
                            }
                        } finally {
                            handlerListener.unbindId(worker);
                        }

                        if (hasWorkingFlag(completionFlag)) {
                            if (events.size() < pollingLimit) {
                                completionFlag = rangeWalker.getWalkingRange().isToDefined() ? RANGE_OVER : SUSPEND;
                            }
                            if (event != null) {
                                moveRange(event);
                            }
                        }
                    }

                } catch (UnsupportedByServiceException e) {
                    log.error("Service adapter doesn't support required operation", e);
                    completionFlag = HANDLER_INTERRUPTION;
                } catch (ServiceException e) {
                    if (e.getCause() instanceof DatasetTooBig) {
                        DatasetTooBig dtbEx = (DatasetTooBig) e.getCause();
                        log.info("Current query size: '{}' is too big, new size is: '{}'", pollingLimit, dtbEx.getLimit());
                        //we shouldn't get into DatasetTooBig often so we can afford waiting an iteration to continue. This can be changed later.
                        pollingLimit = dtbEx.getLimit();
                    } else if (markIfInterrupted(e.getCause())) {
                        log.info("Task interrupted [break]");
                        return;
                    } else {
                        log.warn("Failed to execute request to repository service, caused by: {}", e.getMessage());

                        try {
                            ErrorAction actionType = pollingConfig.getErrorHandler().handleError(subscriptionKey, e);
                            switch (actionType) {
                                case RETRY:
                                    log.warn("Retry request after error");
                                    break;
                                case INTERRUPT:
                                    log.warn("Interrupt request after error");
                                    completionFlag = HANDLER_INTERRUPTION;
                                    break;
                                default:
                                    throw new IllegalStateException("Unknown error action: " + actionType);
                            }
                        } catch (Throwable t) {
                            log.error("Error during error handling", t);
                            markIfInterrupted(t);
                        }
                    }
                } finally {
                    handlerListener.unbindId(worker);
                }

                switch (completionFlag) {
                    case RANGE_OVER:
                        log.debug("Subscription completed");
                        poller.directRemovePolling(subscriptionKey, false);
                        break;
                    case HANDLER_INTERRUPTION:
                        log.debug("Subscription interrupted");
                        poller.directRemovePolling(subscriptionKey, true);
                        break;
                    default:
                        //do nothing
                }
            } catch (Throwable t) {
                log.error("Error during poll processing, task is broken", t);
                if (!markIfInterrupted(t)) {
                    throw new RuntimeException("Task is broken", t);
                }
            } finally {
                LogSupport.removeSubscriptionKey();
            }
        }).run();
    }

    void stop() {
        running = false;
    }

    private int processEvent(TEvent event, int bindingId, Thread worker) throws Exception {
        int completionFlag = WORKING;
        EventHandler<TEvent> eventHandler = pollingConfig.getEventHandler();
        handling:
        while (true) {
            if (!isWorking(worker)) {
                completionFlag = HANDLER_INTERRUPTION;
                break;
            }

            EventAction eventAction;
            handlerListener.beforeHandle(bindingId, event, subscriptionKey);
            try {
                eventAction = eventHandler.handle(event, subscriptionKey);
            } finally {
                handlerListener.afterHandle(bindingId, event, subscriptionKey);
            }
            switch (eventAction) {
                case RETRY:
                    log.info("Handler requested retry on event: {}", event);
                    continue handling;
                case DELAYED_RETRY:
                    log.info("Handler requested delayed retry on event: {}", event);
                    Thread.sleep(pollingConfig.getEventRetryDelay());
                    continue handling;
                case CONTINUE:
                    break handling;
                case INTERRUPT:
                    log.info("Handler requested interruption on event: {}", event);
                    completionFlag = HANDLER_INTERRUPTION;
                    break handling;
                default:
                    throw new IllegalStateException("Unknown action: " + eventAction);
            }
        }
        return completionFlag;
    }

    private void moveRange(final TEvent lastEvent) {
        rangeWalker.moveRange((walker, boundInclusive) -> {
            Comparable val;
            if (walker instanceof IdRangeWalker) {
                val = ValuesExtractor.getEventId(lastEvent);
            } else {
                val = Instant.from(ValuesExtractor.getCreatedAt(lastEvent));
            }
            return new AbstractMap.SimpleEntry(val, false);
        });
        log.debug("Range moved to: {}", rangeWalker);
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

    private <T extends Comparable, R extends EventRange, RW extends RangeWalker> RW initRange(R range, Function<R, RW> walkerCreator, Function<TEvent, T> valExtractor, Supplier<R> emptyRangeSupplier) throws ServiceException {
        log.debug("Trying to initialize range base on: {}", range);
        RW rangeWalker;
        if (range.isFromDefined()) {
            rangeWalker = walkerCreator.apply(range);
        } else {
            TEvent event = !noEventsInit && range.isFromNow() ? serviceAdapter.getLastEvent() : serviceAdapter.getFirstEvent();
            if (event == null) {
                log.trace("No events in stock");
                noEventsInit = true;
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
