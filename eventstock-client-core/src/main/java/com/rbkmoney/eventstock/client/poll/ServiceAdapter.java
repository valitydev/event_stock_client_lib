package com.rbkmoney.eventstock.client.poll;

import java.time.temporal.TemporalAccessor;
import java.util.Collection;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface ServiceAdapter<TEvent, TRange> {
    Collection<TEvent> getEventRange(TRange range, int limit) throws ServiceException;
    TEvent getFirstEvent() throws ServiceException;
    TEvent getLastEvent() throws ServiceException;
    Long getEventId(TEvent event);
    TemporalAccessor getEventCreatedAt(TEvent event);
}
