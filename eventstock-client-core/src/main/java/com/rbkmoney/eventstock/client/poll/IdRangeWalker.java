package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventRange;

class IdRangeWalker extends AbstractRangeWalker<Long> {
    public IdRangeWalker(EventRange<Long> range) {
        super(range);
    }

    @Override
    protected EventRange<Long> createRange(Long fromBound, boolean fromInclusive, Long toBound, boolean toInclusive, EventRange initialRange) {
        return new EventConstraint.EventIDRange(fromBound, fromInclusive, toBound, toInclusive);
    }
}
