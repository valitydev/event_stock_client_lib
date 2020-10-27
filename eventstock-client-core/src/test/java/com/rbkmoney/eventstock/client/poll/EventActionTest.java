package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.eventstock.client.thrift.SinkEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class EventActionTest {
    final int eventRetryDelay = 1200;
    PollingEventPublisher<SinkEvent> publisher;
    CountDownLatch latch;
    List<EventAction> mockReturns;
    List<SinkEvent> acceptedEvents = new ArrayList<>();
    List<Long> acceptedTime = new ArrayList<>();

    @Before
    public void setUp() {
        latch = new CountDownLatch(1);
        mockReturns = new ArrayList<EventAction>() {{
            add(EventAction.CONTINUE);
            add(EventAction.RETRY);
            add(EventAction.DELAYED_RETRY);
            add(EventAction.INTERRUPT);
        }};

        publisher = new DefaultPollingEventPublisherBuilder()
                .withEventHandler(
                        new EventHandler<SinkEvent>() {
                            AtomicInteger counter = new AtomicInteger(0);

                            @Override
                            public EventAction handle(SinkEvent event, String subsKey) {
                                acceptedEvents.add(event);
                                acceptedTime.add(System.currentTimeMillis());
                                return mockReturns.get(counter.getAndIncrement());
                            }

                            @Override
                            public void handleCompleted(String subsKey) {
                                latch.countDown();
                            }

                            @Override
                            public void handleInterrupted(String subsKey) {
                                latch.countDown();
                            }
                        })
                .withErrorHandler((subsKey, errCause) -> {
                    errCause.printStackTrace();
                    return ErrorAction.INTERRUPT;
                })
                .withServiceAdapter(new EventGenerator.ServiceAdapterStub())
                .withEventRetryDelay(eventRetryDelay)
                .build();
    }

    @Test
    public void testActions() throws InterruptedException {
        publisher.subscribe(new DefaultSubscriberConfig<>(
                new EventFlowFilter(
                        new EventConstraint(
                                new EventConstraint.EventIDRange(0L, Long.MAX_VALUE)))
        ));

        latch.await();
        SinkEvent prevEvent = null;
        BiConsumer<SinkEvent, SinkEvent> noCheck = (p, c) -> {
        };
        BiConsumer<SinkEvent, SinkEvent> nextCheck = noCheck;
        long prevHTime, hTime = 0;
        for (EventAction mockReturn : mockReturns) {
            SinkEvent currEvent = acceptedEvents.remove(0);
            prevHTime = hTime;
            hTime = acceptedTime.remove(0);
            nextCheck.accept(prevEvent, currEvent);
            switch (mockReturn) {
                case CONTINUE:
                    nextCheck = noCheck;
                    break;
                case RETRY:
                case DELAYED_RETRY:
                    nextCheck = (p, c) -> Assert.assertSame(p, c);
                    break;
                case INTERRUPT:
                    Assert.assertTrue(eventRetryDelay <= hTime - prevHTime);
                    nextCheck = (p, c) -> Assert.assertTrue(acceptedEvents.isEmpty());
                    break;
            }
            prevEvent = currEvent;
        }
        nextCheck.accept(prevEvent, null);
        publisher.destroy();
    }

}
