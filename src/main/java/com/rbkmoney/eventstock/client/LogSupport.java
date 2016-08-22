package com.rbkmoney.eventstock.client;

import org.slf4j.MDC;

/**
 * Created by vpankrashkin on 20.08.16.
 */
public class LogSupport {
    public static final String SUBSCRIPTION_KEY = "es_subscription_key";

    public static void setSubscriptionKey(String subscriptionKey) {
        if (subscriptionKey == null) {
            MDC.remove(SUBSCRIPTION_KEY);
        } else {
            MDC.put(SUBSCRIPTION_KEY, subscriptionKey);
        }
    }

    public static void removeSubscriptionKey() {
        setSubscriptionKey(null);
    }
}
