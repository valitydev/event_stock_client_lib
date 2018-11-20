package com.rbkmoney.eventstock.client.poll.additional.identity;

import com.rbkmoney.eventstock.client.EventConstraint;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.fistful.identity.SinkEvent;
import com.rbkmoney.geck.filter.Filter;

import java.time.temporal.TemporalAccessor;

public class IdentityEventFilter implements EventFilter<SinkEvent> {

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
