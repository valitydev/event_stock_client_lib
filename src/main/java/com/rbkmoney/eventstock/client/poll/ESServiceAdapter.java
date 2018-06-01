package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.event_stock.EventConstraint;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class ESServiceAdapter implements ServiceAdapter<StockEvent, com.rbkmoney.eventstock.client.EventConstraint> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final EventRepositorySrv.Iface repository;

    public static ESServiceAdapter build(ClientBuilder clientBuilder) {
        return new ESServiceAdapter(clientBuilder.build(EventRepositorySrv.Iface.class));
    }

    public ESServiceAdapter(EventRepositorySrv.Iface repository) {
        this.repository = repository;
    }

    @Override
    public Collection<StockEvent> getEventRange(com.rbkmoney.eventstock.client.EventConstraint srcConstraint, int limit) throws ServiceException {
        EventConstraint resConstraint = convertConstraint(srcConstraint, limit);
        log.debug("New event range request: {}, limit: {}", resConstraint, limit);
        try {
            Collection<StockEvent> events = repository.getEvents(resConstraint);
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
            StockEvent stockEvent = repository.getFirstEvent();
            log.debug("Received event: {}", stockEvent);
            return stockEvent;
        } catch (NoStockEvent e) {
            return null;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public StockEvent getLastEvent() throws ServiceException {
        try {
            log.debug("New last event request");
            StockEvent stockEvent = repository.getLastEvent();
            log.debug("Received event: {}", stockEvent);
            return stockEvent;
        } catch (NoStockEvent e) {
            return null;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    public static EventConstraint convertConstraint(com.rbkmoney.eventstock.client.EventConstraint scrConstraint, int limit) {
        EventRange resRange = new EventRange();
        if (scrConstraint.getIdRange() != null) {
            resRange.setIdRange(convertRange(scrConstraint.getIdRange()));
        } else if (scrConstraint.getTimeRange() != null) {
            resRange.setTimeRange(convertRange(scrConstraint.getTimeRange()));
        }
        return new EventConstraint(resRange, limit);
    }

    private static EventIDRange convertRange(com.rbkmoney.eventstock.client.EventConstraint.EventIDRange srcIdRange) {
        EventIDRange resIdRange = new EventIDRange();

        if (srcIdRange.isFromDefined()) {
            resIdRange.setFromId(srcIdRange.isFromInclusive() ? EventIDBound.inclusive(srcIdRange.getFrom()) : EventIDBound.exclusive(srcIdRange.getFrom()));
        }
        if (srcIdRange.isToDefined()) {
            resIdRange.setToId(srcIdRange.isToInclusive() ? EventIDBound.inclusive(srcIdRange.getTo()) : EventIDBound.exclusive(srcIdRange.getTo()));
        }

        return resIdRange;
    }

    private static EventTimeRange convertRange(com.rbkmoney.eventstock.client.EventConstraint.EventTimeRange srcTimeRange) {
        EventTimeRange resTimeRange = new EventTimeRange();

        if (srcTimeRange.isFromDefined()) {
            String timeStr = TypeUtil.temporalToString(srcTimeRange.getFrom());
            resTimeRange.setFromTime(srcTimeRange.isFromInclusive() ? EventTimeBound.inclusive(timeStr) : EventTimeBound.exclusive(timeStr));
        }
        if (srcTimeRange.isToDefined()) {
            String timeStr = TypeUtil.temporalToString(srcTimeRange.getTo());
            resTimeRange.setToTime(srcTimeRange.isToInclusive() ? EventTimeBound.inclusive(timeStr) : EventTimeBound.exclusive(timeStr));
        }

        return resTimeRange;
    }
}
