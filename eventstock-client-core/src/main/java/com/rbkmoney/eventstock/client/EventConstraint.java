package com.rbkmoney.eventstock.client;

import java.time.Instant;

public class EventConstraint {
    private final EventIDRange idRange;
    private final EventTimeRange timeRange;


    private EventConstraint(EventIDRange idRange, EventTimeRange timeRange) {
        this.idRange = idRange;
        this.timeRange = timeRange;
    }

    public EventConstraint(EventRange newRange) {
        if (newRange instanceof EventIDRange) {
            this.idRange = (EventIDRange) newRange;
            this.timeRange = null;
        } else if (newRange instanceof EventTimeRange) {
            this.timeRange = (EventTimeRange) newRange;
            this.idRange = null;
        } else {
            throw new IllegalArgumentException("Range has unsupported type");
        }
    }

    public EventConstraint(EventIDRange idRange) {
        this(idRange, null);
    }

    public EventConstraint(EventTimeRange timeRange) {
        this(null, timeRange);
    }

    public EventIDRange getIdRange() {
        return idRange;
    }

    public EventTimeRange getTimeRange() {
        return timeRange;
    }

    public boolean accept(Long id, Instant time) {
        boolean inIdRange = (idRange != null ? idRange.accept(id) : true);
        boolean inTimeRange = (timeRange != null ? timeRange.accept(time) : true);
        return inIdRange & inTimeRange;
    }

    public static class EventIDRange extends EventRange<Long> {
        public EventIDRange(Long from, boolean fromInclusiveFlag, Long to, boolean toInclusiveFlag) {
            super(from, fromInclusiveFlag, to, toInclusiveFlag);
        }

        public EventIDRange(Long from, Long to) {
            super(from, true, to, false);
        }

        public EventIDRange() {
            super();
        }
    }

    public static class EventTimeRange extends EventRange<Instant> {
        public EventTimeRange(Instant from, boolean fromInclusiveFlag, Instant to, boolean toInclusiveFlag) {
            super(from, fromInclusiveFlag, to, toInclusiveFlag);
        }

        public EventTimeRange(Instant from, Instant to) {
            super(from, true, to, false);
        }

        public EventTimeRange() {
            super();
        }
    }

    @Override
    public String toString() {
        return "EventConstraint{" +
                "idRange=" + idRange +
                ", timeRange=" + timeRange +
                '}';
    }
}
