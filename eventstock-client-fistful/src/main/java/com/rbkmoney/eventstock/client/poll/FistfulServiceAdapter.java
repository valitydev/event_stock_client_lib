package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.fistful.eventsink.NoLastEvent;
import com.rbkmoney.fistful.withdrawal.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.ClientBuilder;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.List;

public class FistfulServiceAdapter<TEvent> implements ServiceAdapter<TEvent, EventConstraint> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final FistfulRepository<TEvent> repository;

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

            @Override
            public Long getEventId(SinkEvent sinkEvent) {
                return sinkEvent.getId();
            }

            @Override
            public TemporalAccessor getEventCreatedAt(SinkEvent sinkEvent) {
                return TypeUtil.stringToTemporal(sinkEvent.getCreatedAt());
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

            @Override
            public Long getEventId(com.rbkmoney.fistful.identity.SinkEvent sinkEvent) {
                return sinkEvent.getId();
            }

            @Override
            public TemporalAccessor getEventCreatedAt(com.rbkmoney.fistful.identity.SinkEvent sinkEvent) {
                return TypeUtil.stringToTemporal(sinkEvent.getCreatedAt());
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

            @Override
            public Long getEventId(com.rbkmoney.fistful.wallet.SinkEvent sinkEvent) {
                return sinkEvent.getId();
            }

            @Override
            public TemporalAccessor getEventCreatedAt(com.rbkmoney.fistful.wallet.SinkEvent sinkEvent) {
                return TypeUtil.stringToTemporal(sinkEvent.getCreatedAt());
            }
        });
    }

    private FistfulServiceAdapter(FistfulRepository<TEvent> repository) {
        this.repository = repository;
    }

    @Override
    public Collection<TEvent> getEventRange(EventConstraint eventConstraint, int limit) throws ServiceException {
        EventRange eventRange = convertConstraint(eventConstraint, limit);
        log.debug("New event range request: {}, limit: {}", eventRange, limit);
        try {
            Collection<TEvent> events = repository.getEvents(eventRange);
            log.debug("Received events count: {}", events.size());
            log.trace("Received events: {}", events);
            return events;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public TEvent getFirstEvent() throws ServiceException {
        try {
            log.debug("New first event request");
            EventRange range = new EventRange();
            range.setLimit(1);
            List<TEvent> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            TEvent event = events.get(0);
            log.debug("Received event: {}", event);
            return event;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public TEvent getLastEvent() throws ServiceException {
        try {
            log.debug("New last event request");
            long lastId = repository.getLastEventID();
            EventRange range = new EventRange();
            if (lastId > Long.MIN_VALUE) {
                range.setAfter(lastId - 1);
            }
            range.setLimit(1);
            List<TEvent> events = repository.getEvents(range);
            if (events.isEmpty()) {
                return null;
            }
            TEvent event = events.get(0);
            log.debug("Received event: {}", event);
            return event;
        } catch (NoLastEvent e) {
            return null;
        } catch (Exception e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public Long getEventId(TEvent event) {
        return repository.getEventId(event);
    }

    @Override
    public TemporalAccessor getEventCreatedAt(TEvent event) {
        return repository.getEventCreatedAt(event);
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
