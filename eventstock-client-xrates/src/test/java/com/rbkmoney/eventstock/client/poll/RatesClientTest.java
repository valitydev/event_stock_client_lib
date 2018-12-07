package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.DefaultSubscriberConfig;
import com.rbkmoney.eventstock.client.EventAction;
import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.geck.filter.Filter;
import com.rbkmoney.xrates.rate.EventRange;
import com.rbkmoney.xrates.rate.EventSinkSrv;
import com.rbkmoney.xrates.rate.NoLastEvent;
import com.rbkmoney.xrates.rate.SinkEvent;
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

public class RatesClientTest extends AbstractTest {

    @Test
    public void testRatesServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);

        Servlet srv = createThrftRPCService(
                EventSinkSrv.Iface.class,
                createHandler(semaphore),
                null
        );

        addServlet(srv, "/rates");

        PollingEventPublisher publisher = new RatesPollingEventPublisherBuilder()
                .withURI(new URI(getUrlString("/rates")))
                .build();

        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(
                createEventFilter(),
                (e, k) -> {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                }
        );

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    private EventSinkSrv.Iface createHandler(Semaphore semaphore) {
        return new EventSinkSrv.Iface() {

            @Override
            public List<SinkEvent> getEvents(EventRange eventRange) throws TException {
                if (eventRange.getAfter() == -1) {
                    return IntStream.range(0, 3)
                            .mapToObj(RatesEventGenerator::createRatesEvent)
                            .collect(Collectors.toList());
                } else {
                    semaphore.release(1);
                    return Collections.emptyList();
                }
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return 0;
            }

        };
    }

    private EventFilter<SinkEvent> createEventFilter() {
        return new EventFilter<SinkEvent>() {

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
            public boolean accept(Long eventId, TemporalAccessor createdAt, SinkEvent o) {
                return true;
            }
        };
    }
}
