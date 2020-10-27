package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.SourceEvent;
import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.damsel.payment_processing.Event;
import com.rbkmoney.damsel.payment_processing.EventRange;
import com.rbkmoney.damsel.payment_processing.EventSinkSrv;
import com.rbkmoney.damsel.payment_processing.NoLastEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class PPServiceAdapter implements ServiceAdapter<StockEvent, com.rbkmoney.eventstock.client.EventConstraint> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final EventSinkSrv.Iface repository;

    public static PPServiceAdapter build(ClientBuilder clientBuilder) {
        return new PPServiceAdapter(clientBuilder.build(EventSinkSrv.Iface.class));
    }

    public PPServiceAdapter(EventSinkSrv.Iface repository) {
        this.repository = repository;
    }

    @Override
    public Collection<StockEvent> getEventRange(com.rbkmoney.eventstock.client.EventConstraint srcConstraint, int limit) throws ServiceException {

        EventRange eventRange = convertConstraint(srcConstraint, limit);
        log.debug("New event range request: {}, limit: {}", eventRange, limit);
        try {
            Collection<StockEvent> events = repository.getEvents(eventRange).stream().map(this::toStockEvent).collect(Collectors.toList());
            log.debug("Received events count: {}", events.size());
            log.trace("Received events: {}", events);
            return events;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public StockEvent getFirstEvent() throws ServiceException {
        try {
            log.debug("New first event request");
            EventRange range = new EventRange();
            range.setLimit(1);
            List<Event> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            StockEvent stockEvent = toStockEvent(events.get(0));
            log.debug("Received event: {}", stockEvent);
            return stockEvent;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public StockEvent getLastEvent() throws ServiceException {
        try {
            log.debug("New last event request");
            long lastId = repository.getLastEventID();
            EventRange range = new EventRange();
            if (lastId > Long.MIN_VALUE) {
                range.setAfter(lastId - 1);
            }
            range.setLimit(1);
            List<Event> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            StockEvent stockEvent = toStockEvent(events.get(0));
            log.debug("Received event: {}", stockEvent);
            return stockEvent;
        } catch (NoLastEvent e) {
            return null;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public Long getEventId(StockEvent stockEvent) {
        return stockEvent.getSourceEvent().getProcessingEvent().getId();
    }

    @Override
    public TemporalAccessor getEventCreatedAt(StockEvent stockEvent) {
        return TypeUtil.stringToTemporal(stockEvent.getSourceEvent().getProcessingEvent().getCreatedAt());
    }

    private StockEvent toStockEvent(Event event) {
        StockEvent stockEvent = new StockEvent(SourceEvent.processing_event(event));
        stockEvent.setId(event.getId());
        stockEvent.setTime(event.getCreatedAt());
        return stockEvent;
    }

    public static EventRange convertConstraint(com.rbkmoney.eventstock.client.EventConstraint scrConstraint, int limit) throws UnsupportedByServiceException {
        if (scrConstraint.getIdRange() != null) {
            EventRange range = convertRange(scrConstraint.getIdRange());
            range.setLimit(limit);
            return range;
        } else if (scrConstraint.getTimeRange() != null) {
            throw new UnsupportedByServiceException("Time range is not supported by PP interface");
        }
        throw new UnsupportedByServiceException("Unexpected constraint range type: " + scrConstraint);
    }

    private static EventRange convertRange(com.rbkmoney.eventstock.client.EventConstraint.EventIDRange srcIdRange) throws UnsupportedByServiceException {
        EventRange resIdRange = new EventRange();

        if (srcIdRange.isFromDefined()) {
            if (srcIdRange.isFromInclusive()) {
                if (srcIdRange.getFrom() > Long.MIN_VALUE) {
                    resIdRange.setAfter(srcIdRange.getFrom() - 1);//Based on Andrew confirmation that mg doesn't conform to api (no exception thrown on unknown event). Otherwise it's not possible to get specific event by id if preceding gap exists
                }
            } else {
                resIdRange.setAfter(srcIdRange.getFrom());
            }
        }
        if (srcIdRange.isToDefined()) {
            throw new UnsupportedByServiceException("Right Id bound is not supported by PP interface");
        }
        return resIdRange;
    }

}
