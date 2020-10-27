package com.rbkmoney.eventstock.client.poll.additional.withdrawal_session;

import com.rbkmoney.fistful.eventsink.EventRange;
import com.rbkmoney.fistful.withdrawal_session.Change;
import com.rbkmoney.fistful.withdrawal_session.Event;
import com.rbkmoney.fistful.withdrawal_session.Session;
import com.rbkmoney.fistful.withdrawal_session.SinkEvent;
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

public class WithdrawalSessionEventSinkSrv implements com.rbkmoney.fistful.withdrawal_session.EventSinkSrv.Iface {

    private Semaphore semaphore;

    public WithdrawalSessionEventSinkSrv(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    @Override
    public List<SinkEvent> getEvents(EventRange range) {
        if (range.getAfter() == -1) {
            return IntStream.range(0, 3).mapToObj(i -> createWithdrawalSessionEvent(i)).collect(Collectors.toList());
        } else {
            semaphore.release(1);
            return Collections.emptyList();
        }
    }

    public static SinkEvent createWithdrawalSessionEvent(long id) {
        String timeString = TypeUtil.temporalToString(Instant.now());
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setId(id);
        sinkEvent.setCreatedAt(timeString);
        sinkEvent.setPayload(new Event(1, timeString, Arrays.asList(Change.created(new Session()))));

        try {
            sinkEvent = new MockTBaseProcessor(MockMode.REQUIRED_ONLY)
                    .process(sinkEvent, new TBaseHandler<>(SinkEvent.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sinkEvent;
    }

    @Override
    public long getLastEventID() {
        return 0;
    }
}