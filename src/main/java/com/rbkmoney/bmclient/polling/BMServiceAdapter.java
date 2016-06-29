package com.rbkmoney.bmclient.polling;

import com.rbkmoney.damsel.event_stock.EventConstraint;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.event_stock.EventRepositorySrv;
import com.rbkmoney.damsel.event_stock.StockEvent;
import org.apache.thrift.TException;

import java.util.Collection;

/**
 * Created by vpankrashkin on 29.06.16.
 */
public class BMServiceAdapter implements ServiceAdapter<StockEvent, EventRange> {
    private final EventRepositorySrv.Iface repository;

    public BMServiceAdapter(EventRepositorySrv.Iface repository) {
        this.repository = repository;
    }

    @Override
    public Collection<StockEvent> getEventRange(EventRange range, int limit) throws ServiceException {
        EventConstraint eventConstraint = new EventConstraint(range, limit);
        try {
            return repository.getEvents(eventConstraint);
        } catch (TException e) {
            throw new ServiceException(e);
        }
    }
}
