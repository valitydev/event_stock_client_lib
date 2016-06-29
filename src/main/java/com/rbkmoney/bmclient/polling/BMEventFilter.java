package com.rbkmoney.bmclient.polling;

import com.rbkmoney.bmclient.EventFilter;
import com.rbkmoney.damsel.event_stock.EventRange;
import com.rbkmoney.damsel.event_stock.StockEvent;
import com.rbkmoney.thrift.filter.Filter;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public class BMEventFilter implements EventFilter<StockEvent> {
    private final EventRange eventRange;
    private final Filter filter;

    public BMEventFilter(EventRange eventRange) {
        this(eventRange, null);
    }

    public BMEventFilter(EventRange eventRange, Filter filter) {
        this.eventRange = eventRange;
        this.filter = filter;
    }

    @Override
    public boolean accept(StockEvent stockEvent) {
        if (filter != null) {
            return filter.match(stockEvent.getSourceEvent().getProcessingEvent());
        }
        return true;
    }

    public EventRange getEventRange() {
        return eventRange;
    }

    public Filter getFilter() {
        return filter;
    }
}
