package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.eventstock.client.thrift.SinkEvent;
import org.apache.thrift.TBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.System.out;
import static org.hamcrest.CoreMatchers.is;

public class HandlerListenerTest {
    @Test
    public void testHandling() throws InterruptedException {
        int threadsCount = 4;
        int handlerTimeout = 200;
        int hangingPeriod = 2000;
        List<Housekeeper.TimeoutEvent> timeoutEvents = new ArrayList<>();
        List<TBase> expectedTimeoutData = new ArrayList<>();
        long testCount = 6000;

        final AtomicBoolean interrupted = new AtomicBoolean();
        final AtomicInteger prcCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        PollingEventPublisher<SinkEvent> publisher = new DefaultPollingEventPublisherBuilder()
                .withEventHandler(
                        new EventHandler<SinkEvent>() {
                            @Override
                            public EventAction handle(SinkEvent event, String subsKey) {
                                int count = prcCount.incrementAndGet();
                                if (count % hangingPeriod == 0)
                                    try {
                                        expectedTimeoutData.add(event);
                                        Thread.sleep((long) (handlerTimeout * 2.5));
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                return EventAction.CONTINUE;
                            }

                            @Override
                            public void handleCompleted(String subsKey) {
                                interrupted.set(false);
                                latch.countDown();
                            }

                            @Override
                            public void handleInterrupted(String subsKey) {
                                interrupted.set(true);
                                latch.countDown();
                            }
                        })
                .withErrorHandler((subsKey, errCause) -> {
                    errCause.printStackTrace();
                    return ErrorAction.INTERRUPT;
                })
                .withServiceAdapter(new EventGenerator.ServiceAdapterStub())
                .withMaxPoolSize(threadsCount)
                .withHandlerListener(
                        new Housekeeper(
                                (HandlerListener.EventConsumer<Housekeeper.TimeoutEvent>) timeoutEvent ->
                                        timeoutEvents.add(timeoutEvent)
                                ,
                                threadsCount,
                                handlerTimeout)
                )
                .build();
        long startTime = System.currentTimeMillis();
        publisher.subscribe(new DefaultSubscriberConfig<>(
                new EventFlowFilter(
                        new EventConstraint(
                                new EventConstraint.EventIDRange(0L, testCount)))
        ));

        latch.await();
        out.printf("Count: %d, Time: %d\n", testCount, (System.currentTimeMillis() - startTime));
        List timeoutData = timeoutEvents.stream().map(e -> e.getData()).collect(Collectors.toList());
        List uniqTimeoutEvents = (List) timeoutData.stream().distinct().collect(Collectors.toList());
        Assert.assertTrue(timeoutEvents.size() > uniqTimeoutEvents.size());
        Assert.assertThat(expectedTimeoutData, is(uniqTimeoutEvents));
    }

}
