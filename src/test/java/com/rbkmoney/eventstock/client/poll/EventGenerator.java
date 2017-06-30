package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.geck.common.util.TypeUtil;

import java.time.Instant;
import java.util.*;

/**
 * Created by vpankrashkin on 07.06.17.
 */
public class EventGenerator {
    public static StockEvent createStockEvent(long id, boolean flag) {
        return new StockEvent(SourceEvent.processing_event(createEvent(id, flag)));
    }

    public static Event createEvent(long id, boolean flag) {
        String timeString =  TypeUtil.temporalToString(Instant.now());
        Event event = flag ?
                new Event(
                        id,
                        timeString,
                        EventSource.invoice(""+id),
                        0,
                        EventPayload.invoice_event(
                                InvoiceEvent.invoice_created(
                                        new InvoiceCreated(
                                                new Invoice(
                                                        id+"",
                                                        "kek_id",
                                                        "1",
                                                        "kek_time",
                                                        InvoiceStatus.unpaid(new InvoiceUnpaid()),
                                                        new InvoiceDetails("kek_product"),
                                                        "kek_time",
                                                        new Cash(100, new CurrencyRef("RUB"))
                                                )
                                        )
                                )
                        )
                )
                :
                new Event(
                        id,
                        timeString,
                        EventSource.invoice(""+(id-1)),
                        0,
                        EventPayload.invoice_event(
                                InvoiceEvent.invoice_status_changed(
                                        new InvoiceStatusChanged(
                                                InvoiceStatus.unpaid(
                                                        new InvoiceUnpaid())))));
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
                 counter < limit && i+fromId <= (Optional.of(constraint).map(com.rbkmoney.damsel.event_stock.EventConstraint::getEventRange).map(EventRange::getIdRange).map(EventIDRange::getToId).map(EventIDBound::isSetInclusive).orElse(false) ? toId : toId-1);
                 ++i, ++counter) {
                long id = i+fromId;
                if (id <= expectedMax)
                list.add(createStockEvent(id, id % 2 ==0));
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
