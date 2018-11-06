package com.rbkmoney.eventstock.client;

import com.rbkmoney.eventstock.client.poll.ServiceAdapter;
import com.rbkmoney.eventstock.client.poll.ServiceException;
import com.rbkmoney.eventstock.client.thrift.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;

import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class EventGenerator {

    public static List<SinkEvent> createEvents(EventConstraint constraint, long limit, long expectedMax) {
        EventConstraint.EventIDRange idRange = constraint.getIdRange();
        Long fromId = idRange.getFrom();
        Long toId = Optional.of(idRange).map(EventConstraint.EventIDRange::getTo).orElse(null);
        if (fromId >= expectedMax) {
            return Collections.emptyList();
        }
        toId = toId == null ? Long.MAX_VALUE : toId;
        if (fromId >= toId) {
            return Collections.emptyList();
        } else {
            List list = new ArrayList();
            for (long i = (constraint.getIdRange().isFromInclusive() ? 0 : 1), counter = 0;
                 counter < limit && i + fromId <= (Optional.of(constraint).map(EventConstraint::getIdRange).map(EventConstraint.EventIDRange::isToInclusive).orElse(false) ? toId : toId - 1);
                 ++i, ++counter) {
                long id = i + fromId;
                if (id <= expectedMax)
                    list.add(createSinkEvent(id));
            }
            return list;
        }
    }

    public static SinkEvent createSinkEvent(long id) {
        return new SinkEvent(id, Instant.now().toString());
    }

    public static class ServiceAdapterStub implements ServiceAdapter<SinkEvent, EventConstraint> {

        @Override
        public Collection<SinkEvent> getEventRange(EventConstraint eventConstraint, int limit) throws ServiceException {
            return createEvents(eventConstraint, limit, Integer.MAX_VALUE);
        }

        @Override
        public SinkEvent getFirstEvent() throws ServiceException {
            return createSinkEvent(0);
        }

        @Override
        public SinkEvent getLastEvent() throws ServiceException {
            return createSinkEvent(Integer.MAX_VALUE);
        }

        @Override
        public Long getEventId(SinkEvent sinkEvent) {
            return sinkEvent.getEventId();
        }

        @Override
        public TemporalAccessor getEventCreatedAt(SinkEvent sinkEvent) {
            return TypeUtil.stringToTemporal(sinkEvent.getCreatedAt());
        }
    }

}
