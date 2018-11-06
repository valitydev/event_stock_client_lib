package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.fistful.eventsink.NoLastEvent;
import org.apache.thrift.TException;

import java.time.temporal.TemporalAccessor;
import java.util.List;

public interface FistfulRepository<TEvent> {

    List<TEvent> getEvents(EventRange eventRange) throws TException;

    long getLastEventID() throws NoLastEvent, TException;

    Long getEventId(TEvent tEvent);

    TemporalAccessor getEventCreatedAt(TEvent tEvent);

}
