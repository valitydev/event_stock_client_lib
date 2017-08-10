package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.EventConstraint;
import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.woody.api.event.ServiceEvent;
import com.rbkmoney.woody.api.event.ServiceEventListener;
import com.rbkmoney.woody.api.trace.MetadataProperties;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class ClientTest extends AbstractTest {
    private static final Logger log = LoggerFactory.getLogger(ClientTest.class);
    private class ERSImpl implements EventRepositorySrv.Iface {
        final  Long lastEventId = 0L;
        final Long firstEventId = 0L;
        private long expectedMax = Long.MAX_VALUE;
        private AtomicInteger rangeRequestsCount = new AtomicInteger();

        public ERSImpl(long expectedMax) {
            this.expectedMax = expectedMax;
        }

        public ERSImpl() {
        }

        @Override
        public List<StockEvent> getEvents(EventConstraint constraint) throws InvalidRequest, DatasetTooBig, TException {
            log.info("Client: Get Events: {}", constraint);
            rangeRequestsCount.incrementAndGet();
            return EventGenerator.createEvents(constraint, expectedMax);
        }

        @Override
        public StockEvent getLastEvent() throws NoLastEvent, TException {
            log.info("Client: Get last event");
            StockEvent event = new StockEvent(SourceEvent.processing_event(EventGenerator.createEvent(lastEventId, true)));
            log.info("Client: result: {}", event);
            return event;
        }

        @Override
        public StockEvent getFirstEvent() throws NoStockEvent, TException {
            log.info("Client: Get first event");
            StockEvent event = new StockEvent(SourceEvent.processing_event(EventGenerator.createEvent(firstEventId, true)));
            log.info("Client: result: {}", event);
            return event;
        }

        public int getRangeRequestsCount() {
            return rangeRequestsCount.get();
        }
    };

     class EHImpl implements EventHandler<StockEvent> {
        private CountDownLatch latch;
        private Collection eventIds;

        public EHImpl() {
        }

        public EHImpl(CountDownLatch latch, Collection eventIds) {
            this.latch = latch;
            this.eventIds = eventIds;
        }

        @Override
        public EventAction handle(StockEvent event, String subsKey) {
            log.info(subsKey+":Handled object: "+event);
            if (eventIds != null)
            eventIds.add(((StockEvent) event).getSourceEvent().getProcessingEvent().getId());
            return EventAction.CONTINUE;
        }

        @Override
        public void handleCompleted(String subsKey) {
            log.info(subsKey+":No more elements");
            if (latch != null)
            latch.countDown();
        }
    }
    @Test
    public void testLimitedRange() throws URISyntaxException, InterruptedException {
        final List<Long> receivedIdList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        ERSImpl ers = new ERSImpl(1000);

        addServlet(new THServiceBuilder().withEventListener(
                (ServiceEventListener<ServiceEvent>) serviceEvent -> log.info(serviceEvent.getActiveSpan().getMetadata().getValue(MetadataProperties.CALL_ERROR))
        ).build(EventRepositorySrv.Iface.class, ers), "/test");

        PollingEventPublisherBuilder eventPublisherBuilder = new PollingEventPublisherBuilder();
        eventPublisherBuilder.withURI(new URI(getUrlString("/test")));
        eventPublisherBuilder.withEventHandler(new EHImpl(latch, receivedIdList));
        eventPublisherBuilder.withMaxQuerySize(2);

        PollingEventPublisher<StockEvent> eventPublisher = eventPublisherBuilder.build();

        eventPublisher.subscribe(new DefaultSubscriberConfig<>(createEventFilter(0L, 10L, false)));

        latch.await();

        Assert.assertArrayEquals(LongStream.range(0, 10).toArray(), receivedIdList.stream().mapToLong(i -> i).toArray());
        Assert.assertEquals("5 requests return 2 records each, 10 in total, last request to check its over after previous full result", 5+1, ers.getRangeRequestsCount());
        eventPublisher.destroy();
    }

    @Test
    public void testFilteredLimitedRange() throws URISyntaxException, InterruptedException {
        final List<Long> receivedIdList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        ERSImpl ers = new ERSImpl(1000);

        addServlet(new THServiceBuilder().build(EventRepositorySrv.Iface.class, ers), "/test");

        PollingEventPublisherBuilder eventPublisherBuilder = new PollingEventPublisherBuilder();
        eventPublisherBuilder.withEventHandler(new EHImpl(latch, receivedIdList));
        eventPublisherBuilder.withURI(new URI(getUrlString("/test")));
        eventPublisherBuilder.withMaxQuerySize(2);
        eventPublisherBuilder.getClientBuilder().withNetworkTimeout(0);

        PollingEventPublisher<StockEvent> eventPublisher = eventPublisherBuilder.build();

        eventPublisher.subscribe(new DefaultSubscriberConfig<>(createEventFilter(0L, 10L, true)));

        latch.await();

        Assert.assertEquals(5, receivedIdList.size());
        Assert.assertEquals("5 requests return 2 records each, 10 in total, last request to check its over after previous full result", 5+1, ers.getRangeRequestsCount());
        eventPublisher.destroy();
    }

    @Test
    public void testUnlimitedRange() throws URISyntaxException, InterruptedException {
        final List<Long> receivedIdList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        int maxEventId = 20;
        ERSImpl ers = new ERSImpl(maxEventId);
        addServlet(new THServiceBuilder().build(EventRepositorySrv.Iface.class, ers), "/test");

        PollingEventPublisherBuilder eventPublisherBuilder = new PollingEventPublisherBuilder();
        eventPublisherBuilder.withEventHandler(new EHImpl(latch, receivedIdList));
        eventPublisherBuilder.withURI(new URI(getUrlString("/test")));
        eventPublisherBuilder.withMaxQuerySize(15);

        PollingEventPublisher<StockEvent> eventPublisher = eventPublisherBuilder.build();

        eventPublisher.subscribe(new DefaultSubscriberConfig<>(createEventFilter(0L, null, false)));

        Assert.assertFalse(latch.await(5, TimeUnit.SECONDS));
        Assert.assertArrayEquals(LongStream.range(0, maxEventId+1).toArray(), receivedIdList.stream().mapToLong(i -> i).toArray());

        Assert.assertTrue("2 requests to exhaust store, next requests're empty ", ers.getRangeRequestsCount() > 4);
        eventPublisher.destroy();
    }

    @Test
    public void testLimitedRangeWithNoDownBound() throws URISyntaxException, InterruptedException {
        final List<Long> receivedIdList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        ERSImpl ers = new ERSImpl(20);
        addServlet(new THServiceBuilder().build(EventRepositorySrv.Iface.class, ers), "/test");

        PollingEventPublisherBuilder eventPublisherBuilder = new PollingEventPublisherBuilder();
        eventPublisherBuilder.withEventHandler(new EHImpl(latch, receivedIdList));
        eventPublisherBuilder.withURI(new URI(getUrlString("/test")));
        eventPublisherBuilder.withMaxQuerySize(2);

        PollingEventPublisher<StockEvent> eventPublisher = eventPublisherBuilder.build();

        Long upBound = ers.firstEventId + 10;
        eventPublisher.subscribe(new DefaultSubscriberConfig<>(createEventFilter(null, upBound, false)));

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        Assert.assertArrayEquals(LongStream.range(ers.firstEventId, upBound).toArray(), receivedIdList.stream().mapToLong(i -> i).toArray());

        Assert.assertEquals(6, ers.getRangeRequestsCount());
        eventPublisher.destroy();
    }

    @Test
    public void testUnlimitedRangeWithNowDownBound() throws URISyntaxException, InterruptedException {
        final List<Long> receivedIdList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        int maxEventId = 20;
        ERSImpl ers = new ERSImpl(maxEventId);
        addServlet(new THServiceBuilder().build(EventRepositorySrv.Iface.class, ers), "/test");

        PollingEventPublisherBuilder eventPublisherBuilder = new PollingEventPublisherBuilder();
        eventPublisherBuilder.withEventHandler(new EHImpl(latch, receivedIdList));
        eventPublisherBuilder.withURI(new URI(getUrlString("/test")));
        eventPublisherBuilder.withMaxQuerySize(2);

        PollingEventPublisher<StockEvent> eventPublisher = eventPublisherBuilder.build();

        EventFilter eventFilter = createEventFilter(null, null, false);
        eventFilter.getEventConstraint().getIdRange().setFromNow();
        eventPublisher.subscribe(new DefaultSubscriberConfig<>(eventFilter));

        Assert.assertFalse(latch.await(5, TimeUnit.SECONDS));
        Assert.assertArrayEquals("maxEventId+1 -> 1(because last request that its exhausted)", LongStream.range(ers.lastEventId, maxEventId+1).toArray(), receivedIdList.stream().mapToLong(i -> i).toArray());

        Assert.assertTrue(ers.getRangeRequestsCount() > 7);
        eventPublisher.destroy();
    }

}
