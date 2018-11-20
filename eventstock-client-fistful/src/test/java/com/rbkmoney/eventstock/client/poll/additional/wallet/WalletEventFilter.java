package com.rbkmoney.eventstock.client.poll.additional.wallet;

import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.fistful.wallet.SinkEvent;
import com.rbkmoney.geck.filter.Filter;

import java.time.temporal.TemporalAccessor;

public class WalletEventFilter implements EventFilter<SinkEvent> {

	@Override
	public EventConstraint getEventConstraint() {
		EventConstraint.EventIDRange range = new EventConstraint.EventIDRange();
		range.setFromNow();
		return new EventConstraint(range);
	}

	@Override
	public Filter getFilter() {
		return null;
	}

	@Override
	public int getLimit() {
		return 1;
	}

	@Override
	public boolean accept(Long eventId, TemporalAccessor createdAt, SinkEvent o) {
		return true;
	}

}
