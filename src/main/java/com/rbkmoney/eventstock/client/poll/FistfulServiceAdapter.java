package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.fistful.eventsink.NoLastEvent;
import com.rbkmoney.woody.api.ClientBuilder;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class FistfulServiceAdapter<T> implements ServiceAdapter<T, EventConstraint> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final FistfulRepository<T> repository;

    public static FistfulServiceAdapter<com.rbkmoney.fistful.withdrawal.SinkEvent> buildWithdrawalAdapter(ClientBuilder clientBuilder) {
        com.rbkmoney.fistful.withdrawal.EventSinkSrv.Iface client = clientBuilder.build(com.rbkmoney.fistful.withdrawal.EventSinkSrv.Iface.class);
        return new FistfulServiceAdapter<>(new FistfulRepository<com.rbkmoney.fistful.withdrawal.SinkEvent>() {
            @Override
            public List<com.rbkmoney.fistful.withdrawal.SinkEvent> getEvents(EventRange eventRange) throws TException {
                return client.getEvents(eventRange);
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return client.getLastEventID();
            }
        });
    }

    public static FistfulServiceAdapter<com.rbkmoney.fistful.identity.SinkEvent> buildIdentityAdapter(ClientBuilder clientBuilder) {
        com.rbkmoney.fistful.identity.EventSinkSrv.Iface client = clientBuilder.build(com.rbkmoney.fistful.identity.EventSinkSrv.Iface.class);
        return new FistfulServiceAdapter<>(new FistfulRepository<com.rbkmoney.fistful.identity.SinkEvent>() {
            @Override
            public List<com.rbkmoney.fistful.identity.SinkEvent> getEvents(EventRange eventRange) throws TException {
                return client.getEvents(eventRange);
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return client.getLastEventID();
            }
        });
    }

    public static FistfulServiceAdapter<com.rbkmoney.fistful.wallet.SinkEvent> buildWalletAdapter(ClientBuilder clientBuilder) {
        com.rbkmoney.fistful.wallet.EventSinkSrv.Iface client = clientBuilder.build(com.rbkmoney.fistful.wallet.EventSinkSrv.Iface.class);
        return new FistfulServiceAdapter<>(new FistfulRepository<com.rbkmoney.fistful.wallet.SinkEvent>() {
            @Override
            public List<com.rbkmoney.fistful.wallet.SinkEvent> getEvents(EventRange eventRange) throws TException {
                return client.getEvents(eventRange);
            }

            @Override
            public long getLastEventID() throws NoLastEvent, TException {
                return client.getLastEventID();
            }
        });
    }

    private FistfulServiceAdapter(FistfulRepository<T> repository) {
        this.repository = repository;
    }

    @Override
    public Collection<T> getEventRange(EventConstraint eventConstraint, int limit) throws ServiceException {
        EventRange eventRange = convertConstraint(eventConstraint, limit);
        log.debug("New event range request: {}, limit: {}", eventRange, limit);
        try {
            Collection<T> events = repository.getEvents(eventRange);
            log.debug("Received events count: {}", events.size());
            log.trace("Received events: {}", events);
            return events;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public T getFirstEvent() throws ServiceException {
        try {
            log.debug("New first event request");
            EventRange range = new EventRange();
            range.setLimit(1);
            List<T> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            T event = events.get(0);
            log.debug("Received event: {}", event);
            return event;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public T getLastEvent() throws ServiceException {
        try {
            log.debug("New last event request");
            long lastId = repository.getLastEventID();
            EventRange range = new EventRange();
            if (lastId > Long.MIN_VALUE) {
                range.setAfter(lastId - 1);
            }
            range.setLimit(1);
            List<T> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            T event = events.get(0);
            log.debug("Received event: {}", event);
            return event;
        } catch (NoLastEvent e) {
            return null;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
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

    private interface FistfulRepository<TEvent> {

        List<TEvent> getEvents(EventRange eventRange) throws TException;

        long getLastEventID() throws NoLastEvent, TException;

    }
}
