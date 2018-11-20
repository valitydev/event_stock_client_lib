package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.DefaultSubscriberConfig;
import com.rbkmoney.eventstock.client.EventAction;
import com.rbkmoney.eventstock.client.poll.additional.deposit.DepositEventFilter;
import com.rbkmoney.eventstock.client.poll.additional.deposit.DepositEventSinkSrv;
import com.rbkmoney.eventstock.client.poll.additional.destination.DestinationEventFilter;
import com.rbkmoney.eventstock.client.poll.additional.destination.DestinationEventSinkSrv;
import com.rbkmoney.eventstock.client.poll.additional.identity.IdentityEventFilter;
import com.rbkmoney.eventstock.client.poll.additional.identity.IdentityEventSinkSrv;
import com.rbkmoney.eventstock.client.poll.additional.source.SourceEventFilter;
import com.rbkmoney.eventstock.client.poll.additional.source.SourceEventSinkSrv;
import com.rbkmoney.eventstock.client.poll.additional.wallet.WalletEventFilter;
import com.rbkmoney.eventstock.client.poll.additional.wallet.WalletEventSinkSrv;
import com.rbkmoney.eventstock.client.poll.additional.withdrawal.WithdrawalEventFilter;
import com.rbkmoney.eventstock.client.poll.additional.withdrawal.WithdrawalEventSinkSrv;
import com.rbkmoney.eventstock.client.poll.additional.withdrawal_session.WithdrawalSessionEventFilter;
import com.rbkmoney.eventstock.client.poll.additional.withdrawal_session.WithdrawalSessionEventSinkSrv;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.Servlet;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class FistfulClientTest extends AbstractTest {

    @Test
    public void testWalletServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);
        String path = "/wallet";

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.wallet.EventSinkSrv.Iface.class,
                new WalletEventSinkSrv(semaphore), null);
        addServlet(srv, path);

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder();
        builder.withWalletServiceAdapter();
        builder.withURI(new URI(getUrlString(path)));
        PollingEventPublisher publisher = builder.build();

        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new WalletEventFilter(),
                (e, k) ->
                {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testIdentityServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);
        String path = "/identity";

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.identity.EventSinkSrv.Iface.class,
                new IdentityEventSinkSrv(semaphore), null);
        addServlet(srv, path);

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder();
        builder.withIdentityServiceAdapter();
        builder.withURI(new URI(getUrlString(path)));
        PollingEventPublisher publisher = builder.build();

        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new IdentityEventFilter(),
                (e, k) ->
                {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testWithdrawalServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);
        String path = "/withdrawal";

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.withdrawal.EventSinkSrv.Iface.class,
                new WithdrawalEventSinkSrv(semaphore), null);
        addServlet(srv, path);

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder();
        builder.withWithdrawalServiceAdapter();
        builder.withURI(new URI(getUrlString(path)));
        PollingEventPublisher publisher = builder.build();

        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new WithdrawalEventFilter(),
                (e, k) ->
                {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testDepositServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);
        String path = "/deposit";

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.deposit.EventSinkSrv.Iface.class,
                new DepositEventSinkSrv(semaphore), null);
        addServlet(srv, path);

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder();
        builder.withDepositServiceAdapter();
        builder.withURI(new URI(getUrlString(path)));
        PollingEventPublisher publisher = builder.build();

        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new DepositEventFilter(),
                (e, k) ->
                {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testSourceServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);
        String path = "/source";

        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.source.EventSinkSrv.Iface.class,
                new SourceEventSinkSrv(semaphore), null);
        addServlet(srv, path);

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder();
        builder.withSourceServiceAdapter();
        builder.withURI(new URI(getUrlString(path)));
        PollingEventPublisher publisher = builder.build();

        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new SourceEventFilter(),
                (e, k) ->
                {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testDestinationServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);
        String path = "/destination";

        DestinationEventSinkSrv destinationEventSinkSrv = new DestinationEventSinkSrv(semaphore);
        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.destination.EventSinkSrv.Iface.class,
                destinationEventSinkSrv, null);

        addServlet(srv, path);

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder();
        builder.withDestinationServiceAdapter();
        builder.withURI(new URI(getUrlString(path)));
        PollingEventPublisher publisher = builder.build();
        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new DestinationEventFilter(),
                (e, k) ->
                {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

    @Test
    public void testWithdrawalSessionServiceAdapter() throws URISyntaxException, InterruptedException {
        Semaphore semaphore = new Semaphore(-1);
        AtomicLong lastId = new AtomicLong(-1);
        String path = "/withdrawal_session";

        WithdrawalSessionEventSinkSrv iface = new WithdrawalSessionEventSinkSrv(semaphore);
        Servlet srv = createThrftRPCService(com.rbkmoney.fistful.withdrawal_session.EventSinkSrv.Iface.class,
                iface, null);
        addServlet(srv, path);

        FistfulPollingEventPublisherBuilder builder = new FistfulPollingEventPublisherBuilder();
        builder.withWithdrawalSessionServiceAdapter();
        builder.withURI(new URI(getUrlString(path)));
        PollingEventPublisher publisher = builder.build();

        DefaultSubscriberConfig config = new DefaultSubscriberConfig<>(new WithdrawalSessionEventFilter(),
                (e, k) ->
                {
                    lastId.set(e.getId());
                    return EventAction.CONTINUE;
                });

        publisher.subscribe(config);
        semaphore.acquire(1);
        Assert.assertEquals(2, lastId.get());
        publisher.destroy();
    }

}
