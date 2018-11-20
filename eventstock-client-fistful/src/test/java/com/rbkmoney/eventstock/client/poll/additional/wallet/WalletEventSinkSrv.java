package com.rbkmoney.eventstock.client.poll.additional.wallet;

import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.fistful.wallet.SinkEvent;
import com.rbkmoney.fistful.wallet.Event;
import com.rbkmoney.fistful.wallet.Change;
import com.rbkmoney.fistful.wallet.Wallet;
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

public class WalletEventSinkSrv implements com.rbkmoney.fistful.wallet.EventSinkSrv.Iface {

	private Semaphore semaphore;

	public WalletEventSinkSrv(Semaphore semaphore) {
		this.semaphore = semaphore;
	}

	@Override
	public List<SinkEvent> getEvents(EventRange range) {
		if (range.getAfter() == -1) {
			return IntStream.range(0, 3).mapToObj(i -> createWalletEvent(i)).collect(Collectors.toList());
		} else {
			semaphore.release(1);
			return Collections.emptyList();
		}
	}

	public static SinkEvent createWalletEvent(long id) {
		String timeString = TypeUtil.temporalToString(Instant.now());
		SinkEvent sinkEvent = new SinkEvent();
		sinkEvent.setId(id);
		sinkEvent.setCreatedAt(timeString);
		sinkEvent.setPayload(new Event(1, timeString, Arrays.asList(Change.created(new Wallet()))));
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
