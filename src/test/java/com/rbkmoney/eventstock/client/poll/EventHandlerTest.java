package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.eventstock.client.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
/**
 * Created by vpankrashkin on 07.06.17.
 */
public class EventHandlerTest {
    PollingEventPublisher<StockEvent> publisher;
    CountDownLatch latch;
    List<EventAction> mockReturns;
    List<StockEvent> acceptedEvents = new ArrayList<>();

    @Before
    public void setUp() {
        latch = new CountDownLatch(1);
        mockReturns = new ArrayList<EventAction>(){{
            add(EventAction.CONTINUE);
            add(EventAction.RETRY);
            add(EventAction.INTERRUPT);
        }};

        publisher = new DefaultPollingEventPublisherBuilder()
                .withEventHandler(
                        new EventHandler<StockEvent>() {
                            AtomicInteger counter = new AtomicInteger(0);
                            @Override
                            public EventAction handle(StockEvent event, String subsKey) {
                                acceptedEvents.add(event);
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
                .build();
    }

    @Test
    public void test() throws InterruptedException {
        publisher.subscribe(new DefaultSubscriberConfig<>(
                new EventFlowFilter(
                        new EventConstraint(
                                new EventConstraint.EventIDRange(0L, Long.MAX_VALUE)))
        ));

        latch.await();
        StockEvent prevEvent = null;
        BiConsumer<StockEvent, StockEvent> noCheck = (p, c) -> {};
        BiConsumer<StockEvent, StockEvent> nextCheck = noCheck;
        for (EventAction mockReturn: mockReturns) {
            StockEvent currEvent = acceptedEvents.remove(0);
            nextCheck.accept(prevEvent, currEvent);
            switch (mockReturn) {
                case CONTINUE:
                    nextCheck = noCheck;
                    break;
                case RETRY:
                    nextCheck = (p, c) -> Assert.assertSame(p, c);
                    break;
                case INTERRUPT:
                    nextCheck = (p, c) -> Assert.assertTrue(acceptedEvents.isEmpty());
                    break;
            }
            prevEvent = currEvent;
        }
        nextCheck.accept(prevEvent, null);
        publisher.destroy();

    }

}
