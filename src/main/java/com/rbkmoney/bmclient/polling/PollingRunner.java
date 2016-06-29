package com.rbkmoney.bmclient.polling;

/**
 * Created by vpankrashkin on 28.06.16.
 */
public interface PollingRunner {
    boolean addPolling(String subsKey, SubscriberInfo subscriberInfo, int maxBlockSize);
    boolean removePolling(String subsKey);
    void destroy();
}
