package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.domain.Currency;
import com.rbkmoney.damsel.event_stock.EventConstraint;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.payment_processing.Event;
import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.domain.Currency;
import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.eventstock.client.DefaultSubscriberConfig;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.eventstock.client.EventHandler;
import com.rbkmoney.thrift.filter.Filter;
import com.rbkmoney.thrift.filter.PathConditionFilter;
import com.rbkmoney.thrift.filter.condition.Relation;
import com.rbkmoney.thrift.filter.converter.TemporalConverter;
import com.rbkmoney.thrift.filter.rule.PathConditionRule;
import com.rbkmoney.woody.api.event.*;
import com.rbkmoney.woody.api.trace.MetadataProperties;
import com.rbkmoney.woody.api.trace.context.TraceContext;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.time.Instant;
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
            return createEvents(constraint, expectedMax);
        }

        @Override
        public StockEvent getLastEvent() throws NoLastEvent, TException {
            log.info("Client: Get last event");
            StockEvent event = new StockEvent(SourceEvent.processing_event(createEvent(lastEventId, true)));
            log.info("Client: result: {}", event);
            return event;
        }

        @Override
        public StockEvent getFirstEvent() throws NoStockEvent, TException {
            log.info("Client: Get first event");
            StockEvent event = new StockEvent(SourceEvent.processing_event(createEvent(firstEventId, true)));
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
        public void handleEvent(StockEvent event, String subsKey) {
            log.info(subsKey+":Handled object: "+event);
            if (eventIds != null)
            eventIds.add(((StockEvent) event).getSourceEvent().getProcessingEvent().getId());
        }

        @Override
        public void handleNoMoreElements(String subsKey) {
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

    private EventFilter createEventFilter(Long from, Long to, boolean addFilter) {
        com.rbkmoney.eventstock.client.EventRange eventRange = new com.rbkmoney.eventstock.client.EventConstraint.EventIDRange();
        eventRange.setFromInclusive(from);
        eventRange.setToExclusive(to);
        Filter filter = !addFilter ? null : new PathConditionFilter(new PathConditionRule("payload.invoice_event.invoice_status_changed.status", new com.rbkmoney.thrift.filter.condition.CompareCondition("unpaid", Relation.EQ)));
        EventFlowFilter eventFlowFilter = new EventFlowFilter(new com.rbkmoney.eventstock.client.EventConstraint(eventRange), filter);
        return eventFlowFilter;
    }

    private Event createEvent(long id, boolean flag) {
        String timeString =  TemporalConverter.temporalToString(Instant.now());
        Event event = flag ?
                new Event(
                        id,
                        timeString,
                        EventSource.invoice(""+id),
                        0,
                        EventPayload.invoice_event(
                                InvoiceEvent.invoice_created(
                                        new InvoiceCreated(
                                                new Invoice(
                                                        id+"",
                                                        new PartyRef("1", 1),
                                                        "1",
                                                        "kek",
                                                        1,
                                                        InvoiceStatus.unpaid(new InvoiceUnpaid()),
                                                        "kek",
                                                        "kek",
                                                        new Cash(100, new Currency("", "RUB", (short) 1, (short) 0))
                                                )
                                        )
                                )
                        )
                )
                :
                new Event(
                        id,
                        timeString,
                        EventSource.invoice(""+(id-1)),
                        0,
                        EventPayload.invoice_event(
                                InvoiceEvent.invoice_status_changed(
                                        new InvoiceStatusChanged(
                                                InvoiceStatus.unpaid(
                                                        new InvoiceUnpaid())))));
        return event;
    }

    private List<StockEvent> createEvents(EventConstraint constraint, long expectedMax) {
        EventIDRange idRange = constraint.getEventRange().getIdRange();
        Long fromId = (Long) idRange.getFromId().getFieldValue();
        Long toId = (Long) Optional.of(idRange).map(EventIDRange::getToId).map(EventIDBound::getFieldValue).orElse(null);
        if (fromId >= expectedMax) {
            return Collections.emptyList();
        }
        toId = toId == null ? Long.MAX_VALUE : toId;
        int limit = constraint.getLimit();
        if (fromId >= toId) {
            return Collections.emptyList();
        } else {
            List list = new ArrayList();
            for (long i = (constraint.getEventRange().getIdRange().getFromId().isSetInclusive() ? 0 : 1), counter = 0;
                 counter < limit && i+fromId <= (Optional.of(constraint).map(EventConstraint::getEventRange).map(EventRange::getIdRange).map(EventIDRange::getToId).map(EventIDBound::isSetInclusive).orElse(false) ? toId : toId-1);
                 ++i, ++counter) {
                long id = i+fromId;
                if (id <= expectedMax)
                list.add(new StockEvent(SourceEvent.processing_event(createEvent(id, id % 2 ==0))));
            }
            return list;
        }
    }
}
