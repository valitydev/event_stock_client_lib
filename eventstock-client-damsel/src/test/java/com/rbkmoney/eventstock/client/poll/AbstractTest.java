package com.rbkmoney.eventstock.client.poll;


import com.rbkmoney.damsel.event_stock.*;
import com.rbkmoney.eventstock.client.EventFilter;
import com.rbkmoney.geck.filter.Filter;
import com.rbkmoney.geck.filter.PathConditionFilter;
import com.rbkmoney.geck.filter.condition.IsNullCondition;
import com.rbkmoney.geck.filter.rule.PathConditionRule;
import com.rbkmoney.woody.api.event.ClientEventListener;
import com.rbkmoney.woody.api.event.ServiceEventListener;
import com.rbkmoney.woody.api.generator.IdGenerator;
import com.rbkmoney.woody.thrift.impl.http.THClientBuilder;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServlet;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Created by vpankrashkin on 06.05.16.
 */
public class AbstractTest {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private HandlerCollection handlerCollection;
    protected Server server;
    protected int serverPort = -1;
    protected TProcessor tProcessor;

    @Before
    public void startJetty() throws Exception {

        server = new Server(0);
        HandlerCollection contextHandlerCollection = new HandlerCollection(true); // important! use parameter
        // mutableWhenRunning==true
        this.handlerCollection = contextHandlerCollection;
        server.setHandler(contextHandlerCollection);

        server.start();
        serverPort = ((NetworkConnector) server.getConnectors()[0]).getLocalPort();
    }

    protected void addServlet(Servlet servlet, String mapping) {
        try {
            ServletContextHandler context = new ServletContextHandler();
            ServletHolder defaultServ = new ServletHolder(mapping, servlet);
            context.addServlet(defaultServ, mapping);
            handlerCollection.addHandler(context);
            context.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void stopJetty() {
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected String getUrlString() {
        return "http://localhost:" + serverPort;
    }

    public TServlet createTServlet(TProcessor tProcessor) {
        return new TServlet(tProcessor, new TBinaryProtocol.Factory());
    }

    public TServlet createMutableTervlet() {
        return new TServlet(new TProcessor() {
            @Override
            public boolean process(TProtocol in, TProtocol out) throws TException {
                return tProcessor.process(in, out);
            }
        }, new TBinaryProtocol.Factory());
    }

    protected <T> Servlet createThrftRPCService(Class<T> iface, T handler, ServiceEventListener eventListener) {
        THServiceBuilder serviceBuilder = new THServiceBuilder();
        if (eventListener != null) {
            serviceBuilder.withEventListener(eventListener);
        }
        return serviceBuilder.build(iface, handler);
    }

    protected String getUrlString(String contextPath) {
        return getUrlString() + contextPath;
    }


    protected <T> T createThriftRPCClient(Class<T> iface, IdGenerator idGenerator, ClientEventListener eventListener) {
        return createThriftRPCClient(iface, idGenerator, eventListener, getUrlString());
    }


    protected <T> T createThriftRPCClient(Class<T> iface, IdGenerator idGenerator, ClientEventListener eventListener, String url) {
        try {
            THClientBuilder clientBuilder = new THClientBuilder();
            clientBuilder.withAddress(new URI(url));
            clientBuilder.withHttpClient(HttpClientBuilder.create().build());
            if (idGenerator != null) {
                clientBuilder.withIdGenerator(idGenerator);
            }
            if (eventListener != null) {
                clientBuilder.withEventListener(eventListener);
            }
            return clientBuilder.build(iface);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    protected static List<StockEvent> createEvents(EventConstraint constraint, long expectedMax) {
        EventIDRange idRange = constraint.getEventRange().getIdRange();
        Long fromId = (Long) idRange.getFromId().getFieldValue();
        Long toId = (Long) Optional.of(idRange).map(EventIDRange::getToId).map(EventIDBound::getFieldValue).orElse(null);
        if (fromId >= expectedMax) {
            return Collections.emptyList();
        }
        toId = toId == null ? Long.MAX_VALUE : toId;
        int limit = constraint.getLimit();
        if (fromId >= toId) {
            return Collections.emptyList();
        } else {
            List list = new ArrayList();
            for (long i = (constraint.getEventRange().getIdRange().getFromId().isSetInclusive() ? 0 : 1), counter = 0;
                 counter < limit && i + fromId <= (Optional.of(constraint).map(EventConstraint::getEventRange).map(com.rbkmoney.damsel.event_stock.EventRange::getIdRange).map(EventIDRange::getToId).map(EventIDBound::isSetInclusive).orElse(false) ? toId : toId - 1);
                 ++i, ++counter) {
                long id = i + fromId;
                if (id <= expectedMax)
                    list.add(new StockEvent(SourceEvent.processing_event(EventGenerator.createEvent(id, id % 2 == 0))));
            }
            return list;
        }
    }

    protected EventFilter createEventFilter(Long from, Long to, boolean addFilter) {
        com.rbkmoney.eventstock.client.EventRange eventRange = new com.rbkmoney.eventstock.client.EventConstraint.EventIDRange();
        eventRange.setFromInclusive(from);
        eventRange.setToExclusive(to);
        Filter filter = !addFilter ? null : new PathConditionFilter(new PathConditionRule("source_event.processing_event.payload.invoice_changes.invoice_status_changed.status.unpaid", new IsNullCondition().not()));
        EventFlowFilter eventFlowFilter = new EventFlowFilter(new com.rbkmoney.eventstock.client.EventConstraint(eventRange), filter);
        return eventFlowFilter;
    }


}
