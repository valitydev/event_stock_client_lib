package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

public class FistfulEventGenerator {

    public static com.rbkmoney.fistful.withdrawal.SinkEvent createWithdrawalEvent(long id) {
        String timeString = TypeUtil.temporalToString(Instant.now());
        com.rbkmoney.fistful.withdrawal.SinkEvent sinkEvent = new com.rbkmoney.fistful.withdrawal.SinkEvent();
        sinkEvent.setId(id);
        sinkEvent.setCreatedAt(timeString);
        sinkEvent.setPayload(
                new com.rbkmoney.fistful.withdrawal.Event(
                        1,
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
        sinkEvent.setId(id);
        sinkEvent.setCreatedAt(timeString);
        sinkEvent.setPayload(
                new com.rbkmoney.fistful.identity.Event(
                        1,
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
        sinkEvent.setId(id);
        sinkEvent.setCreatedAt(timeString);
        sinkEvent.setPayload(
                new com.rbkmoney.fistful.wallet.Event(
                        1,
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

}
