package com.rbkmoney.bmclient.polling;

import java.util.Collection;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface ServiceAdapter<TEvent, TRange> {
    Collection<TEvent> getEventRange(TRange range, int limit) throws ServiceException;
}
