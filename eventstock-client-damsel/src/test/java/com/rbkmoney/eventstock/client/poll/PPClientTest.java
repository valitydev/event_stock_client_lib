package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.base.InvalidRequest;
import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.eventstock.client.DefaultSubscriberConfig;
import com.rbkmoney.eventstock.client.EventAction;
import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.geck.filter.Filter;
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

public class PPClientTest extends AbstractTest {

    @Test
    public void testPPServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(EventSinkSrv.Iface.class, new EventSinkSrv.Iface() {
            @Override
            public List<Event> getEvents(EventRange range) throws EventNotFound, InvalidRequest, TException {
                if (range.getAfter() == -1) {
                    return IntStream.range(0, 3).mapToObj(i -> EventGenerator.createEvent(i, true)).collect(Collectors.toList());
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

        addServlet(srv, "/pp");

        PollingEventPublisherBuilder builder = new PollingEventPublisherBuilder().withPPServiceAdapter();
        builder.withURI(new URI(getUrlString("/pp")));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<StockEvent>(new EventFilter() {
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
            public boolean accept(Long id, TemporalAccessor createdAt, Object o) {
                return true;
            }
        }, (e, k) -> {
            lastId.set(e.getId());
            return EventAction.CONTINUE;
        });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
    }

}
