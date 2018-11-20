package com.rbkmoney.eventstock.client.poll.repositories;

import com.rbkmoney.eventstock.client.poll.FistfulRepository;
import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.fistful.wallet.EventSinkSrv.Iface;
import com.rbkmoney.fistful.wallet.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.woody.api.ClientBuilder;
import org.apache.thrift.TException;

import java.time.temporal.TemporalAccessor;
import java.util.List;

public class WalletFistfulRepository implements FistfulRepository<SinkEvent> {

	private final Iface client;

	public WalletFistfulRepository(ClientBuilder clientBuilder) {
		client = clientBuilder.build(Iface.class);
	}

	@Override
	public List<SinkEvent> getEvents(EventRange eventRange) throws TException {
		return client.getEvents(eventRange);
	}

	@Override
	public long getLastEventID() throws TException {
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

}
