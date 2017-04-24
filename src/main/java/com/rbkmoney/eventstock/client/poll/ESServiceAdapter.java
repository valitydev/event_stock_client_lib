package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.event_stock.EventConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.rbkmoney.eventstock.client.APIConversionUtil.convertConstraint;

/**
 * Created by vpankrashkin on 29.06.16.
 */
class ESServiceAdapter implements ServiceAdapter<StockEvent, com.rbkmoney.eventstock.client.EventConstraint> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final EventRepositorySrv.Iface repository;

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

}
