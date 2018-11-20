package com.rbkmoney.eventstock.client.poll.additional.deposit;

import com.rbkmoney.fistful.deposit.SinkEvent;
import com.rbkmoney.fistful.deposit.Event;
import com.rbkmoney.fistful.deposit.Change;
import com.rbkmoney.fistful.deposit.Deposit;
import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DepositEventSinkSrv implements com.rbkmoney.fistful.deposit.EventSinkSrv.Iface {

	private Semaphore semaphore;

	public DepositEventSinkSrv(Semaphore semaphore) {
		this.semaphore = semaphore;
	}

	@Override
	public List<SinkEvent> getEvents(EventRange range) {
		if (range.getAfter() == -1) {
			return IntStream.range(0, 3).mapToObj(i -> createDepositEvent(i)).collect(Collectors.toList());
		} else {
			semaphore.release(1);
			return Collections.emptyList();
		}
	}

	public static SinkEvent createDepositEvent(long id) {
		String timeString = TypeUtil.temporalToString(Instant.now());
		SinkEvent sinkEvent = new SinkEvent();
		sinkEvent.setId(id);
		sinkEvent.setCreatedAt(timeString);
		sinkEvent.setPayload(new Event(1, timeString, Arrays.asList(Change.created(new Deposit()))));
		try {
			TBaseHandler<SinkEvent> handler = new TBaseHandler<>(SinkEvent.class);
			return new MockTBaseProcessor(MockMode.REQUIRED_ONLY).process(sinkEvent, handler);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long getLastEventID() {
		return 0;
	}

}
