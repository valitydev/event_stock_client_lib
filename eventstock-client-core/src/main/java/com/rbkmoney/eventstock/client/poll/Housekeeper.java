package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.HandlerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class Housekeeper<T extends Object> implements HandlerListener<T> {
    private static final Logger log = LoggerFactory.getLogger(Housekeeper.class);

    private final int parallelism;
    private final WorkerState[] states;
    private final ConcurrentHashMap<Thread, Integer> workerMapping;
    private final long timeout;
    private final EventConsumer<TimeoutEvent<T>> eventConsumer;
    private final Thread runnerThread;
    private volatile boolean running = true;

    public Housekeeper(int parallelism, long timeout) {
        this(
                event ->
                        log.warn("Event handler exceeds time limit: [SubsKey: {}, Time(ms): {}, Event: {}]",
                                event.getSubscriptionKey(),
                                event.getTotalTime(),
                                event.getData()),
                parallelism,
                timeout
        );
    }

    public Housekeeper(EventConsumer<TimeoutEvent<T>> eventConsumer, int parallelism, long timeout) {
        Objects.requireNonNull(eventConsumer, "EventConsumer is null");
        this.eventConsumer = eventConsumer;
        this.parallelism = parallelism;
        if (timeout == Long.MAX_VALUE || timeout <= 0) {
            throw new IllegalArgumentException("Timeout value is out of range: " + timeout);
        }
        this.timeout = timeout;
        this.workerMapping = new ConcurrentHashMap<>(parallelism, 0.5f, parallelism);
        WorkerState[] tmpStates = new WorkerState[parallelism];

        for (int i = 0; i < parallelism; ++i) {
            tmpStates[i] = new WorkerState();
            tmpStates[i].timeout = Long.MAX_VALUE;
        }
        this.states = tmpStates;
        this.runnerThread = new Thread(this::run, "ESCHousekeeper");
        this.runnerThread.setDaemon(true);
        this.runnerThread.start();
    }

    private void run() {
        try {
            long prevTime = -1;
            while (running && !Thread.currentThread().isInterrupted()) {
                long time = System.currentTimeMillis();
                if (prevTime > 0 && time - prevTime > timeout * 1.5) {
                    log.warn("Monitor thread starvation, wakeup period hasn't been observed");
                }
                prevTime = time;
                long pause = wakeup(time);
                Thread.sleep(pause);
            }
        } catch (InterruptedException e) {
            log.debug("{} thread is interrupted", Thread.currentThread());
        }
    }

    private long wakeup(long time) {
        WorkerState state;
        long nextTimeout = Long.MAX_VALUE;
        TimeoutEvent[] dataArr = new TimeoutEvent[parallelism];
        int notifyCount = 0;
        for (int i = 0; i < parallelism; ++i) {
            state = states[i];
            synchronized (state) {
                if (state.timeout < Long.MAX_VALUE) {
                    long diff = state.timeout - time;
                    if (diff >= 0) {
                        nextTimeout = nextTimeout < diff ? nextTimeout : diff;
                    } else {
                        dataArr[notifyCount++] = new TimeoutEvent(state.subsKey, state.data, timeout - diff);
                    }
                }
            }
        }
        for (int i = 0; i < notifyCount; ++i) {
            try {
                eventConsumer.consume(dataArr[i]);
            } catch (Throwable t) {
                log.error("EventConsumer failed at: {}", dataArr[i], t);

            }
        }
        return nextTimeout == Long.MAX_VALUE ? this.timeout : nextTimeout;
    }


    @Override
    public void beforeHandle(int bindingId, T event, String subsKey) {
        if (bindingId >= 0) {
            WorkerState state = states[bindingId];
            synchronized (state) {
                state.timeout = System.currentTimeMillis() + timeout;
                state.data = event;
                state.subsKey = subsKey;
            }
        }
    }

    @Override
    public void afterHandle(int bindingId, T event, String subsKey) {
        if (bindingId >= 0) {
            WorkerState state = states[bindingId];
            synchronized (state) {
                state.timeout = Long.MAX_VALUE;
                state.data = null;
                state.subsKey = null;
            }
        }
    }

    @Override
    public int bindId(Thread worker) {
        Integer id = workerMapping.computeIfAbsent(worker, w -> {
            int i = (int) (w.getId() % parallelism);
            if (!states[i].occupied) {
                return i;
            }
            for (i = 0; i < parallelism; ++i) {
                if (!states[i].occupied) {
                    states[i].occupied = true;
                    return i;
                }
            }
            return -1;
        });

        if (id < 0) {
            log.warn("No free slots to bind thread: {}", worker.toString());
        }
        return id;

    }

    @Override
    public void unbindId(Thread worker) {
        workerMapping.computeIfPresent(worker, (w, id) -> {
            states[id].occupied = false;
            return null;
        });
    }

    @Override
    public void destroy() {
        running = false;
        runnerThread.interrupt();
    }

    public static class TimeoutEvent<T> {
        private final String subscriptionKey;
        private final T data;
        private final long totalTime;

        public TimeoutEvent(String subscriptionKey, T data, long totalTime) {
            this.subscriptionKey = subscriptionKey;
            this.data = data;
            this.totalTime = totalTime;
        }

        public String getSubscriptionKey() {
            return subscriptionKey;
        }

        public T getData() {
            return data;
        }

        public long getTotalTime() {
            return totalTime;
        }

        @Override
        public String toString() {
            return "TimeoutEvent{" +
                    "subscriptionKey='" + subscriptionKey + '\'' +
                    ", data=" + data +
                    ", totalTime=" + totalTime +
                    '}';
        }
    }

    private static class WorkerState {
        private boolean occupied;
        private long timeout;
        private Object data;
        private String subsKey;
    }

}
