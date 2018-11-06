package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.event_stock.EventConstraint;
import com.rbkmoney.damsel.payment_processing.NoLastEvent;
import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.woody.api.event.ServiceEvent;
import com.rbkmoney.woody.api.event.ServiceEventListener;
import com.rbkmoney.woody.api.event.ServiceEventType;
import com.rbkmoney.woody.api.trace.MetadataProperties;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Created by vpankrashkin on 21.04.17.
 */
public class LoosingInitStartTest extends AbstractTest {
    final Long initEventId = 0L;
    final int skipEvents = 2;
    int expectedMax = 5;

    private class ERSImpl implements EventRepositorySrv.Iface {
        private AtomicLong currIndex = new AtomicLong();
        private AtomicBoolean wasRequest = new AtomicBoolean(false);
        List<StockEvent> events;

        @Override
        public List<StockEvent> getEvents(EventConstraint constraint) throws InvalidRequest, DatasetTooBig, TException {
            log.info("Client: Get Events: {}", constraint);
            currIndex.set(constraint.getEventRange().getIdRange().getFromId().isSetInclusive() ?
                            constraint.getEventRange().getIdRange().getFromId().getInclusive() :
                    constraint.getEventRange().getIdRange().getFromId().getExclusive() + 1
            );
            if (currIndex.get() > expectedMax) {
                return Collections.emptyList();
            }
            int from = (int) currIndex.get();
            int to = (int) Math.min(currIndex.addAndGet(constraint.getLimit()), (expectedMax));
            List<StockEvent> resEvents = events.subList(from, to);
            return resEvents;
        }

        @Override
        public StockEvent getLastEvent() throws NoLastEvent, TException {
            log.info("Client: Get last event");
            StockEvent event = createAndSkipEventsList(false);
            log.info("Client: result: {}", event);
            return event;
        }

        @Override
        public StockEvent getFirstEvent() throws NoStockEvent, TException {
            log.info("Client: Get first event");
            StockEvent event = createAndSkipEventsList(true);
            log.info("Client: result: {}", event);
            return event;
        }

        private StockEvent createAndSkipEventsList(boolean getFirst) throws NoStockEvent {
            if (wasRequest.compareAndSet(false, true)) {
                throw new NoStockEvent();
            }
            com.rbkmoney.eventstock.client.EventConstraint constraint = new com.rbkmoney.eventstock.client.EventConstraint(
                    new com.rbkmoney.eventstock.client.EventConstraint.EventIDRange() {
                        {
                            setFromInclusive(initEventId);
                            setToInclusive((long)expectedMax);
                        }
                    });
            events = createEvents(ESServiceAdapter.convertConstraint(constraint, expectedMax), expectedMax);

            StockEvent event = events.get((int)currIndex.addAndGet(getFirst ? 0 : skipEvents));
            return event;
        }

    }

    @Test
    public void testLimitedRange() throws URISyntaxException, InterruptedException {
        final List<Long> receivedIdList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        ERSImpl ers = new ERSImpl();

        addServlet(new THServiceBuilder().withEventListener(
                (ServiceEventListener<ServiceEvent>) serviceEvent -> {
                    if (serviceEvent.getEventType() == ServiceEventType.ERROR) {
                        log.info("Call err {}", (Throwable)serviceEvent.getActiveSpan().getMetadata().getValue(MetadataProperties.CALL_ERROR));
                    }
                }
        ).build(EventRepositorySrv.Iface.class, ers), "/test");

        PollingEventPublisherBuilder eventPublisherBuilder = new PollingEventPublisherBuilder();
        eventPublisherBuilder.withURI(new URI(getUrlString("/test")));
        eventPublisherBuilder.withEventHandler(new EventHandler<StockEvent>() {
            @Override
            public EventAction handle(StockEvent event, String subsKey) {
                receivedIdList.add(event.getSourceEvent().getProcessingEvent().getId());
                return EventAction.CONTINUE;
            }

            @Override
            public void handleCompleted(String subsKey) {
                latch.countDown();
            }

            @Override
            public void handleInterrupted(String subsKey) {
                latch.countDown();
            }
        });

        eventPublisherBuilder.withMaxQuerySize(2);

        PollingEventPublisher<StockEvent> eventPublisher = eventPublisherBuilder.build();

        EventFilter filter = createEventFilter(initEventId, (long)expectedMax, false);
        filter.getEventConstraint().getIdRange().setFromNow();
        eventPublisher.subscribe(new DefaultSubscriberConfig<>(filter));
        latch.await();

        Assert.assertEquals(LongStream.range(initEventId, expectedMax).boxed().collect(Collectors.toList()), receivedIdList);
        eventPublisher.destroy();
    }
}
