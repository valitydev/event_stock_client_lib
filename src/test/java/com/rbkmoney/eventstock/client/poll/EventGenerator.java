package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.domain.Invoice;
import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * Created by vpankrashkin on 07.06.17.
 */
public class EventGenerator {
    public static StockEvent createStockEvent(long id, boolean flag) {
        return new StockEvent(SourceEvent.processing_event(createEvent(id, flag)));
    }

    public static com.rbkmoney.fistful.withdrawal.SinkEvent createWithdrawalEvent(long id) {
        String timeString = TypeUtil.temporalToString(Instant.now());
        com.rbkmoney.fistful.withdrawal.SinkEvent sinkEvent = new com.rbkmoney.fistful.withdrawal.SinkEvent();
        sinkEvent.setCreatedAt(timeString);
        sinkEvent.setPayload(
                new com.rbkmoney.fistful.withdrawal.Event(
                        id,
                        timeString,
                        Arrays.asList(
                                com.rbkmoney.fistful.withdrawal.Change.created(new com.rbkmoney.fistful.withdrawal.Withdrawal())
                        )
                )
        );
        try {
            sinkEvent = new MockTBaseProcessor(MockMode.REQUIRED_ONLY).process(sinkEvent, new TBaseHandler<>(com.rbkmoney.fistful.withdrawal.SinkEvent.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sinkEvent;
    }

    public static com.rbkmoney.fistful.identity.SinkEvent createIdentityEvent(long id) {
        String timeString = TypeUtil.temporalToString(Instant.now());
        com.rbkmoney.fistful.identity.SinkEvent sinkEvent = new com.rbkmoney.fistful.identity.SinkEvent();
        sinkEvent.setCreatedAt(timeString);
        sinkEvent.setPayload(
                new com.rbkmoney.fistful.identity.Event(
                        id,
                        timeString,
                        Arrays.asList(
                                com.rbkmoney.fistful.identity.Change.created(new com.rbkmoney.fistful.identity.Identity())
                        )
                )
        );
        try {
            sinkEvent = new MockTBaseProcessor(MockMode.REQUIRED_ONLY).process(sinkEvent, new TBaseHandler<>(com.rbkmoney.fistful.identity.SinkEvent.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sinkEvent;
    }

    public static com.rbkmoney.fistful.wallet.SinkEvent createWalletEvent(long id) {
        String timeString = TypeUtil.temporalToString(Instant.now());
        com.rbkmoney.fistful.wallet.SinkEvent sinkEvent = new com.rbkmoney.fistful.wallet.SinkEvent();
        sinkEvent.setCreatedAt(timeString);
        sinkEvent.setPayload(
                new com.rbkmoney.fistful.wallet.Event(
                        id,
                        timeString,
                        Arrays.asList(
                                com.rbkmoney.fistful.wallet.Change.created(new com.rbkmoney.fistful.wallet.Wallet())
                        )
                )
        );
        try {
            sinkEvent = new MockTBaseProcessor(MockMode.REQUIRED_ONLY).process(sinkEvent, new TBaseHandler<>(com.rbkmoney.fistful.wallet.SinkEvent.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sinkEvent;
    }

    public static Event createEvent(long id, boolean flag) {
        String timeString = TypeUtil.temporalToString(Instant.now());
        Event event = flag ? new Event(id, timeString, EventSource.invoice_id("" + id), EventPayload.invoice_changes(Arrays.asList(
                InvoiceChange.invoice_created(new InvoiceCreated(new InvoiceCreated(new Invoice(
                        id + "",
                        "kek_id",
                        "1",
                        "kek_time",
                        InvoiceStatus.unpaid(new InvoiceUnpaid()),
                        new InvoiceDetails("kek_product"),
                        "kek_time",
                        new Cash(100, new CurrencyRef("RUB"))
                ))))
        ))) : new Event(id, timeString, EventSource.invoice_id("" + id), EventPayload.invoice_changes(Arrays.asList(
                InvoiceChange.invoice_status_changed(new InvoiceStatusChanged(InvoiceStatus.unpaid(new InvoiceUnpaid())))
        )));

        try {
            event = new MockTBaseProcessor(MockMode.REQUIRED_ONLY).process(event, new TBaseHandler<>(Event.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return event;
    }

    public static List<StockEvent> createEvents(com.rbkmoney.damsel.event_stock.EventConstraint constraint, long expectedMax) {
        EventIDRange idRange = constraint.getEventRange().getIdRange();
        Long fromId = (Long) idRange.getFromId().getFieldValue();
        Long toId = (Long) Optional.of(idRange).map(EventIDRange::getToId).map(EventIDBound::getFieldValue).orElse(null);
        if (fromId >= expectedMax) {
            return Collections.emptyList();
        }
        toId = toId == null ? Long.MAX_VALUE : toId;
        int limit = constraint.getLimit();
        if (fromId >= toId) {
            return Collections.emptyList();
        } else {
            List list = new ArrayList();
            for (long i = (constraint.getEventRange().getIdRange().getFromId().isSetInclusive() ? 0 : 1), counter = 0;
                 counter < limit && i + fromId <= (Optional.of(constraint).map(com.rbkmoney.damsel.event_stock.EventConstraint::getEventRange).map(EventRange::getIdRange).map(EventIDRange::getToId).map(EventIDBound::isSetInclusive).orElse(false) ? toId : toId - 1);
                 ++i, ++counter) {
                long id = i + fromId;
                if (id <= expectedMax)
                    list.add(createStockEvent(id, id % 2 == 0));
            }
            return list;
        }
    }

    public static class ServiceAdapterStub implements ServiceAdapter<StockEvent, com.rbkmoney.eventstock.client.EventConstraint> {

        @Override
        public Collection<StockEvent> getEventRange(com.rbkmoney.eventstock.client.EventConstraint eventConstraint, int limit) throws ServiceException {
            return createEvents(ESServiceAdapter.convertConstraint(eventConstraint, limit), Integer.MAX_VALUE);
        }

        @Override
        public StockEvent getFirstEvent() throws ServiceException {
            return createStockEvent(0, false);
        }

        @Override
        public StockEvent getLastEvent() throws ServiceException {
            return createStockEvent(Integer.MAX_VALUE, false);
        }
    }
}
