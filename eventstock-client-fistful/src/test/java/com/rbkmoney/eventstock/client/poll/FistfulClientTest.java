package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.DefaultSubscriberConfig;
import com.rbkmoney.eventstock.client.EventAction;
import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.fistful.eventsink.NoLastEvent;
import com.rbkmoney.geck.filter.Filter;
import com.rbkmoney.woody.api.ClientBuilder;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.Servlet;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FistfulClientTest extends AbstractTest {

    @Test
    public void testWalletServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.wallet.EventSinkSrv.Iface.class, new com.rbkmoney.fistful.wallet.EventSinkSrv.Iface() {
            @Override
            public List<com.rbkmoney.fistful.wallet.SinkEvent> getEvents(EventRange range) throws TException {
                if (range.getAfter() == -1) {
                    return IntStream.range(0, 3).mapToObj(i -> FistfulEventGenerator.createWalletEvent(i)).collect(Collectors.toList());
                } else {
                    semaphore.release(1);
                    return Collections.emptyList();
                }
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return 0;
            }
        }, null);

        addServlet(srv, "/wallet");

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder().withWalletServiceAdapter();
        builder.withURI(new URI(getUrlString("/wallet")));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new EventFilter<com.rbkmoney.fistful.wallet.SinkEvent>() {
            @Override
            public EventConstraint getEventConstraint() {
                EventConstraint.EventIDRange range = new EventConstraint.EventIDRange();
                range.setFromNow();
                return new EventConstraint(range);
            }

            @Override
            public Filter getFilter() {
                return null;
            }

            @Override
            public int getLimit() {
                return 1;
            }

            @Override
            public boolean accept(Long eventId, TemporalAccessor createdAt, com.rbkmoney.fistful.wallet.SinkEvent o) {
                return true;
            }
        }, (e, k) -> {
            lastId.set(e.getId());
            return EventAction.CONTINUE;
        });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testIdentityServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.identity.EventSinkSrv.Iface.class, new com.rbkmoney.fistful.identity.EventSinkSrv.Iface() {
            @Override
            public List<com.rbkmoney.fistful.identity.SinkEvent> getEvents(EventRange range) throws TException {
                if (range.getAfter() == -1) {
                    return IntStream.range(0, 3).mapToObj(i -> FistfulEventGenerator.createIdentityEvent(i)).collect(Collectors.toList());
                } else {
                    semaphore.release(1);
                    return Collections.emptyList();
                }
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return 0;
            }
        }, null);

        addServlet(srv, "/identity");

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder().withIdentityServiceAdapter();
        builder.withURI(new URI(getUrlString("/identity")));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new EventFilter<com.rbkmoney.fistful.identity.SinkEvent>() {
            @Override
            public EventConstraint getEventConstraint() {
                EventConstraint.EventIDRange range = new EventConstraint.EventIDRange();
                range.setFromNow();
                return new EventConstraint(range);
            }

            @Override
            public Filter getFilter() {
                return null;
            }

            @Override
            public int getLimit() {
                return 1;
            }

            @Override
            public boolean accept(Long eventId, TemporalAccessor createdAt, com.rbkmoney.fistful.identity.SinkEvent o) {
                return true;
            }
        }, (e, k) -> {
            lastId.set(e.getId());
            return EventAction.CONTINUE;
        });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testWithdrawalServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.withdrawal.EventSinkSrv.Iface.class, new com.rbkmoney.fistful.withdrawal.EventSinkSrv.Iface() {
            @Override
            public List<com.rbkmoney.fistful.withdrawal.SinkEvent> getEvents(EventRange range) throws TException {
                if (range.getAfter() == -1) {
                    return IntStream.range(0, 3).mapToObj(i -> FistfulEventGenerator.createWithdrawalEvent(i)).collect(Collectors.toList());
                } else {
                    semaphore.release(1);
                    return Collections.emptyList();
                }
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return 0;
            }
        }, null);

        addServlet(srv, "/withdrawal");

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder().withWithdrawalServiceAdapter();
        builder.withURI(new URI(getUrlString("/withdrawal")));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new EventFilter<com.rbkmoney.fistful.withdrawal.SinkEvent>() {
            @Override
            public EventConstraint getEventConstraint() {
                EventConstraint.EventIDRange range = new EventConstraint.EventIDRange();
                range.setFromNow();
                return new EventConstraint(range);
            }

            @Override
            public Filter getFilter() {
                return null;
            }

            @Override
            public int getLimit() {
                return 1;
            }

            @Override
            public boolean accept(Long eventId, TemporalAccessor createdAt, com.rbkmoney.fistful.withdrawal.SinkEvent o) {
                return true;
            }
        }, (e, k) -> {
            lastId.set(e.getId());
            return EventAction.CONTINUE;
        });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testDepositServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.deposit.EventSinkSrv.Iface.class, new com.rbkmoney.fistful.deposit.EventSinkSrv.Iface() {
            @Override
            public List<com.rbkmoney.fistful.deposit.SinkEvent> getEvents(EventRange range) throws TException {
                if (range.getAfter() == -1) {
                    return IntStream.range(0, 3).mapToObj(i -> FistfulEventGenerator.createDepositEvent(i)).collect(Collectors.toList());
                } else {
                    semaphore.release(1);
                    return Collections.emptyList();
                }
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return 0;
            }
        }, null);

        addServlet(srv, "/deposit");

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder().withDepositServiceAdapter();
        builder.withURI(new URI(getUrlString("/deposit")));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new EventFilter<com.rbkmoney.fistful.deposit.SinkEvent>() {
            @Override
            public EventConstraint getEventConstraint() {
                EventConstraint.EventIDRange range = new EventConstraint.EventIDRange();
                range.setFromNow();
                return new EventConstraint(range);
            }

            @Override
            public Filter getFilter() {
                return null;
            }

            @Override
            public int getLimit() {
                return 1;
            }

            @Override
            public boolean accept(Long eventId, TemporalAccessor createdAt, com.rbkmoney.fistful.deposit.SinkEvent o) {
                return true;
            }
        }, (e, k) -> {
            lastId.set(e.getId());
            return EventAction.CONTINUE;
        });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testSourceServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.source.EventSinkSrv.Iface.class, new com.rbkmoney.fistful.source.EventSinkSrv.Iface() {
            @Override
            public List<com.rbkmoney.fistful.source.SinkEvent> getEvents(EventRange range) throws TException {
                if (range.getAfter() == -1) {
                    return IntStream.range(0, 3).mapToObj(i -> FistfulEventGenerator.createSourceEvent(i)).collect(Collectors.toList());
                } else {
                    semaphore.release(1);
                    return Collections.emptyList();
                }
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return 0;
            }
        }, null);

        addServlet(srv, "/source");

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder().withSourceServiceAdapter();
        builder.withURI(new URI(getUrlString("/source")));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new EventFilter<com.rbkmoney.fistful.source.SinkEvent>() {
            @Override
            public EventConstraint getEventConstraint() {
                EventConstraint.EventIDRange range = new EventConstraint.EventIDRange();
                range.setFromNow();
                return new EventConstraint(range);
            }

            @Override
            public Filter getFilter() {
                return null;
            }

            @Override
            public int getLimit() {
                return 1;
            }

            @Override
            public boolean accept(Long eventId, TemporalAccessor createdAt, com.rbkmoney.fistful.source.SinkEvent o) {
                return true;
            }
        }, (e, k) -> {
            lastId.set(e.getId());
            return EventAction.CONTINUE;
        });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testDestinationServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.destination.EventSinkSrv.Iface.class, new com.rbkmoney.fistful.destination.EventSinkSrv.Iface() {
            @Override
            public List<com.rbkmoney.fistful.destination.SinkEvent> getEvents(EventRange range) throws TException {
                if (range.getAfter() == -1) {
                    return IntStream.range(0, 3).mapToObj(i -> FistfulEventGenerator.createDestinationEvent(i)).collect(Collectors.toList());
                } else {
                    semaphore.release(1);
                    return Collections.emptyList();
                }
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return 0;
            }
        }, null);

        addServlet(srv, "/destination");

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder().withDestinationServiceAdapter();
        builder.withURI(new URI(getUrlString("/destination")));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new EventFilter<com.rbkmoney.fistful.destination.SinkEvent>() {
            @Override
            public EventConstraint getEventConstraint() {
                EventConstraint.EventIDRange range = new EventConstraint.EventIDRange();
                range.setFromNow();
                return new EventConstraint(range);
            }

            @Override
            public Filter getFilter() {
                return null;
            }

            @Override
            public int getLimit() {
                return 1;
            }

            @Override
            public boolean accept(Long eventId, TemporalAccessor createdAt, com.rbkmoney.fistful.destination.SinkEvent o) {
                return true;
            }
        }, (e, k) -> {
            lastId.set(e.getId());
            return EventAction.CONTINUE;
        });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

}
