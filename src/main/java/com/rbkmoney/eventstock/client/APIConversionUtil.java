package com.rbkmoney.eventstock.client;

import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.thrift.filter.converter.TemporalConverter;

/**
 * Created by vpankrashkin on 21.04.17.
 */
public class APIConversionUtil {

    public static com.rbkmoney.damsel.event_stock.EventConstraint convertConstraint(com.rbkmoney.eventstock.client.EventConstraint scrConstraint, int limit) {
        com.rbkmoney.damsel.event_stock.EventRange resRange = new EventRange();
        if (scrConstraint.getIdRange() != null) {
            resRange.setIdRange(convertRange(scrConstraint.getIdRange()));
        } else if (scrConstraint.getTimeRange() != null) {
            resRange.setTimeRange(convertRange(scrConstraint.getTimeRange()));
        }
        return new com.rbkmoney.damsel.event_stock.EventConstraint(resRange, limit);
    }

    public static EventIDRange convertRange(com.rbkmoney.eventstock.client.EventConstraint.EventIDRange srcIdRange) {
        EventIDRange resIdRange = new EventIDRange();

        if (srcIdRange.isFromDefined()) {
            resIdRange.setFromId(srcIdRange.isFromInclusive() ? EventIDBound.inclusive(srcIdRange.getFrom()) : EventIDBound.exclusive(srcIdRange.getFrom()));
        }
        if (srcIdRange.isToDefined()) {
            resIdRange.setToId(srcIdRange.isToInclusive() ? EventIDBound.inclusive(srcIdRange.getTo()) : EventIDBound.exclusive(srcIdRange.getTo()));
        }

        return resIdRange;
    }

    public static EventTimeRange convertRange(com.rbkmoney.eventstock.client.EventConstraint.EventTimeRange srcTimeRange) {
        EventTimeRange resTimeRange = new EventTimeRange();

        if (srcTimeRange.isFromDefined()) {
            String timeStr = TemporalConverter.temporalToString(srcTimeRange.getFrom());
            resTimeRange.setFromTime(srcTimeRange.isFromInclusive() ? EventTimeBound.inclusive(timeStr) : EventTimeBound.exclusive(timeStr));
        }
        if (srcTimeRange.isToDefined()) {
            String timeStr = TemporalConverter.temporalToString(srcTimeRange.getTo());
            resTimeRange.setToTime(srcTimeRange.isToInclusive() ? EventTimeBound.inclusive(timeStr) : EventTimeBound.exclusive(timeStr));
        }

        return resTimeRange;
    }
}
