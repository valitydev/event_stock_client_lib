package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.woody.api.event.ClientEventListener;
import com.rbkmoney.woody.api.event.ServiceEventListener;
import com.rbkmoney.woody.api.generator.IdGenerator;
import com.rbkmoney.woody.thrift.impl.http.THClientBuilder;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServlet;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;

import javax.servlet.Servlet;
import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
public class AbstractTest {

    private HandlerCollection handlerCollection;
    protected Server server;
    protected int serverPort = -1;
    protected TProcessor tProcessor;

    @Before
    public void startJetty() throws Exception {
        server = new Server(0);
        HandlerCollection contextHandlerCollection = new HandlerCollection(true);
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
        return new TServlet(
                (in, out) -> tProcessor.process(in, out),
                new TBinaryProtocol.Factory()
        );
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

}
