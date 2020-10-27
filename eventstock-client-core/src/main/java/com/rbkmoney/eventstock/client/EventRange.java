package com.rbkmoney.eventstock.client;

/**
 * Represents range of values. Range is forward oriented and doesn't support rewinding.
 */
public class EventRange<T extends Comparable> {
    private T from;
    private T to;
    private boolean fromNowFlag;
    private boolean fromInclusiveFlag;
    private boolean toInclusiveFlag;

    public EventRange(T from, boolean fromInclusiveFlag, T to, boolean toInclusiveFlag) {
        if (from == null && to == null) {
            throw new NullPointerException("Both bounds cannot be null");
        }
        this.from = from;
        this.to = to;
        this.fromInclusiveFlag = fromInclusiveFlag;
        this.toInclusiveFlag = toInclusiveFlag;
    }

    public EventRange() {
    }

    /**
     * Set the down bound value.
     * If value is null, range is down-bounded with first existing event.
     */
    public void setFrom(T val, boolean inclusive) {
        this.from = val;
        this.fromInclusiveFlag = inclusive;
        this.fromNowFlag = false;
    }

    /**
     * Set the down bound to last existing event, if no such event found, it'll be waited for.
     */
    public void setFromNow() {
        setFrom(null, true);
        this.fromNowFlag = true;
    }

    public void setFromValue(T val) {
        setFrom(val, isFromInclusive());
    }

    public void setToValue(T val) {
        setTo(val, isToInclusive());
    }

    /**
     * Set the up bound value.
     * If value is null, range is not up-bounded.
     */
    public void setTo(T val, boolean inclusive) {
        this.to = val;
        this.toInclusiveFlag = inclusive;
    }


    public void setFromInclusive(T val) {
        setFrom(val, true);
    }

    public void setFromExclusive(T val) {
        setFrom(val, false);
    }

    public void setToInclusive(T val) {
        setTo(val, true);
    }

    public void setToExclusive(T val) {
        setTo(val, false);
    }


    public T getFrom() {
        return from;
    }

    public T getTo() {
        return to;
    }

    public boolean isFromInclusive() {
        return fromInclusiveFlag;
    }

    public boolean isToInclusive() {
        return toInclusiveFlag;
    }

    public boolean isFromNow() {
        return fromNowFlag;
    }

    public boolean isFromDefined() {
        return from != null;
    }

    public boolean isToDefined() {
        return to != null;
    }

    /**
     * @return true - if range has both up and down ranges; false - otherwise.
     */
    public boolean isDefined() {
        return isFromDefined() && isToDefined();
    }

    protected boolean isIn(T val, boolean inclusive) {
        if (getFrom() != null && val != null) {
            int cmpResult = val.compareTo(getFrom());
            if (!(cmpResult == 0 ? !(inclusive ^ isFromInclusive()) : cmpResult > 0)) {
                return false;
            }
        }

        if (getTo() != null && val != null) {
            int cmpResult = val.compareTo(getTo());
            if (!(cmpResult == 0 ? !(inclusive ^ isToInclusive()) : cmpResult < 0)) {
                return false;
            }
        }
        return true;
    }

    public boolean accept(T val) {
        if (val == null) {
            return false;
        }
        return isIn(val, true);
    }

    public boolean isIntersected(EventRange<T> range) {
        boolean fromIn = isIn(range.getFrom(), range.isFromInclusive());
        boolean toIn = isIn(range.getTo(), range.isToInclusive());
        //ranges like [1, 2] and (1, 2) considered as intersected too because logic is based on assumption that we don't know the difference between bounds as real value, we know only relations between bounds and values.
        return (fromIn | toIn) ? true : (range.getFrom().compareTo(getFrom()) <= 0 && range.getTo().compareTo(getTo()) >= 0);
    }

    @Override
    public String toString() {
        return "EventRange{" +
                "from=" + from +
                ", to=" + to +
                ", fromNowFlag=" + fromNowFlag +
                ", fromInclusiveFlag=" + fromInclusiveFlag +
                ", toInclusiveFlag=" + toInclusiveFlag +
                '}';
    }
}
