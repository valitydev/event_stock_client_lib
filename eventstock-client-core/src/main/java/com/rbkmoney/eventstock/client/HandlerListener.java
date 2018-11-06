package com.rbkmoney.eventstock.client;

/**
 * Created by vpankrashkin on 08.06.17.
 */
public interface HandlerListener<EType> {
    void beforeHandle(int bindingId, EType event, String subsKey);
    void afterHandle(int bindingId, EType event, String subsKey);
    int bindId(Thread worker);
    void unbindId(Thread worker);
    void destroy();

    interface EventConsumer<E> {
        void consume(E event);
    }
}
