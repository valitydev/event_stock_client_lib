package com.rbkmoney.eventstock.client.poll;

import java.util.Collection;

/**
 * Created by vpankrashkin on 28.06.16.
 */
interface ServiceAdapter<TEvent, TRange> {
    Collection<TEvent> getEventRange(TRange range, int limit) throws ServiceException;
    TEvent getFirstEvent() throws ServiceException;
    TEvent getLastEvent() throws ServiceException;
}
