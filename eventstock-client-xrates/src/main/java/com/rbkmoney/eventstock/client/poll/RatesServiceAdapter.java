package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.ClientBuilder;
import com.rbkmoney.xrates.rate.EventRange;
import com.rbkmoney.xrates.rate.EventSinkSrv;
import com.rbkmoney.xrates.rate.NoLastEvent;
import com.rbkmoney.xrates.rate.SinkEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.List;

@Slf4j
public class RatesServiceAdapter implements ServiceAdapter<SinkEvent, EventConstraint> {

    private EventSinkSrv.Iface repository;

    public static RatesServiceAdapter build(ClientBuilder clientBuilder) {
        return new RatesServiceAdapter(clientBuilder.build(EventSinkSrv.Iface.class));
    }

    private RatesServiceAdapter(EventSinkSrv.Iface repository) {
        this.repository = repository;
    }

    @Override
    public Collection<SinkEvent> getEventRange(EventConstraint eventConstraint, int limit) throws ServiceException {
        EventRange eventRange = convertConstraint(eventConstraint, limit);
        log.debug("New event range request: {}, limit: {}", eventRange, limit);
        try {
            Collection<SinkEvent> events = repository.getEvents(eventRange);
            log.debug("Received events count: {}", events.size());
            log.trace("Received events: {}", events);
            return events;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public SinkEvent getFirstEvent() throws ServiceException {
        try {
            log.debug("New first event request");
            EventRange range = new EventRange();
            range.setLimit(1);
            List<SinkEvent> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            SinkEvent event = events.get(0);
            log.debug("Received event: {}", event);
            return event;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public SinkEvent getLastEvent() throws ServiceException {
        try {
            log.debug("New last event request");
            long lastId = repository.getLastEventID();
            EventRange range = new EventRange();
            if (lastId > Long.MIN_VALUE) {
                range.setAfter(lastId - 1);
            }
            range.setLimit(1);
            List<SinkEvent> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            SinkEvent event = events.get(0);
            log.debug("Received event: {}", event);
            return event;
        } catch (NoLastEvent e) {
            return null;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public Long getEventId(SinkEvent event) {
        return event.getId();
    }

    @Override
    public TemporalAccessor getEventCreatedAt(SinkEvent event) {
        return TypeUtil.stringToTemporal(event.getCreatedAt());
    }

    public static EventRange convertConstraint(EventConstraint scrConstraint, int limit) throws UnsupportedByServiceException {
        if (scrConstraint.getIdRange() != null) {
            EventRange range = convertRange(scrConstraint.getIdRange());
            range.setLimit(limit);
            return range;
        } else if (scrConstraint.getTimeRange() != null) {
            throw new UnsupportedByServiceException("Time range is not supported by fistful interface");
        }
        throw new UnsupportedByServiceException("Unexpected constraint range type: " + scrConstraint);
    }

    private static EventRange convertRange(EventConstraint.EventIDRange srcIdRange) throws UnsupportedByServiceException {
        EventRange resIdRange = new EventRange();

        if (srcIdRange.isFromDefined()) {
            if (srcIdRange.isFromInclusive()) {
                if (srcIdRange.getFrom() > Long.MIN_VALUE) {
                    resIdRange.setAfter(srcIdRange.getFrom() - 1);
                }
            } else {
                resIdRange.setAfter(srcIdRange.getFrom());
            }
        }
        if (srcIdRange.isToDefined()) {
            throw new UnsupportedByServiceException("Right Id bound is not supported by fistful interface");
        }
        return resIdRange;
    }
}
