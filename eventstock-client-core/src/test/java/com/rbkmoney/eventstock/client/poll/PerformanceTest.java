package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.*;
import com.rbkmoney.eventstock.client.thrift.SinkEvent;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.System.out;

/**
 * Created by vpankrashkin on 07.06.17.
 */
@Ignore
public class PerformanceTest {
    @Test
    public void testHandling() throws InterruptedException {
        final long testCount = 1000000;
        final AtomicBoolean interrupted = new AtomicBoolean();
        final AtomicInteger prcCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        PollingEventPublisher<SinkEvent> publisher = new DefaultPollingEventPublisherBuilder()
                .withEventHandler(
                        new EventHandler<SinkEvent>() {
                            @Override
                            public EventAction handle(SinkEvent event, String subsKey) {
                                prcCount.incrementAndGet();
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
                .withHandlerListener(new Housekeeper(1, 5000))
                .withMaxQuerySize(1000)
                .build();
        long startTime = System.currentTimeMillis();
        publisher.subscribe(new DefaultSubscriberConfig<>(
                new EventFlowFilter(
                        new EventConstraint(
                                new EventConstraint.EventIDRange(0L, testCount)))
        ));

        latch.await();
        out.printf("Count: %d, Time: %d\n", testCount, (System.currentTimeMillis() - startTime));
    }

    static class Data {
        long expireTime;
        Object msg;
    }

    static class Stub {
        Data freeObject = new Data();
        Data usedObject = new Data();
        final AtomicReference<Data> currObject = new AtomicReference<>(usedObject);
    }

    @Test
    public void testSingleLock() {

        try {
            int threadsCount = 4;
            int lockDepth = 1;
            int iterations = 100_000_000;
            Lock[] rLocks = new Lock[threadsCount];
            Lock[] wLocks = new Lock[threadsCount];
            Stub[] stubs = new Stub[threadsCount];

            ReadWriteLock singleLock = new ReentrantReadWriteLock();
            for (int i = 0; i < threadsCount; ++i) {
                //ReadWriteLock lock = singleLock;
                ReadWriteLock lock = new ReentrantReadWriteLock();
                //rLocks[i] = lock.readLock();
                rLocks[i] = new ReentrantLock();
                wLocks[i] = rLocks[i];
                //wLocks[i] = lock.writeLock();
                stubs[i] = new Stub();
            }
            Phaser phaser = new Phaser(threadsCount + 1) {
                @Override
                protected boolean onAdvance(int phase, int registeredParties) {
                    return phase >= 1 && super.onAdvance(phase, registeredParties);
                }
            };

            ExecutorService executor = Executors.newFixedThreadPool(threadsCount + 1);
            for (int i = 0; i < threadsCount; ++i) {
                executor.submit(createTask(iterations, lockDepth, stubs[i], rLocks[i], phaser));
            }
            executor.submit(() -> {
                while (true) {
                    long tmp;
                    for (Stub stub : stubs) {
                        synchronized (stub) {
                            tmp = stub.usedObject.expireTime;
                        }
                    }
                    Thread.sleep(100);
                }
            });

            out.printf("!On barrier: %d, t: %s\n", phaser.getArrivedParties(), Thread.currentThread().getName());

            phaser.arriveAndAwaitAdvance();
            long startTime = System.currentTimeMillis();
            out.printf("Started at: %d\n", startTime);

            phaser.arriveAndAwaitAdvance();
            out.println("Finish Time: " + (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Runnable createTask(final int iterations, final int lCount, Stub stub, Lock lock, Phaser phaser) {
        return () -> {
            try {
                out.printf("On barrier: %d, t: %s\n", phaser.getArrivedParties(), Thread.currentThread().getName());
                phaser.arriveAndAwaitAdvance();
                int tmp = 0;
                int tmp2 = 0;
                for (int t = 0; t < iterations; ++t) {
                    for (int j = 0; j < lCount; ++j) {
                        //lock.lock();
                        try {
                            synchronized (stub) {

                                tmp = j;
                                ++tmp;
                                tmp2 = tmp;
                            }
                            //stub.freeObject.expireTime = j;
                            //stub.freeObject = stub.currObject.getAndSet(stub.freeObject);
                        } finally {
                            // lock.unlock();
                        }
                    }

                }
                out.println("Before OUT" + tmp2);
                phaser.arriveAndAwaitAdvance();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

}
