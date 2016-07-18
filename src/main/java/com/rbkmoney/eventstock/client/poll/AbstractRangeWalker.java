package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.EventRange;
import javafx.util.Pair;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by vpankrashkin on 12.07.16.
 */
abstract  class AbstractRangeWalker<T extends Comparable> implements RangeWalker<T, EventRange<T>> {
    private final EventRange<T> range;
    private EventRange<T> walkingRange;

    public AbstractRangeWalker(EventRange<T> range) {
        this.range = range;
        this.walkingRange = range;
    }

    @Override
    public EventRange<T> getRange() {
        return range;
    }

    @Override
    public EventRange<T> getWalkingRange() {
        return walkingRange;
    }

    @Override
    public EventRange<T> setRange(Function<RangeWalker<T, EventRange<T>>, EventRange<T>> function) {
        walkingRange = function.apply(this);
        return walkingRange;
    }

    @Override
    public EventRange moveRange(BiFunction<RangeWalker<T, EventRange<T>>, Boolean, Pair<T, Boolean>> function) {
        Pair<T, Boolean> fromBoundPair = function.apply(this, walkingRange.isFromInclusive());
        T walkingFromBound = fromBoundPair.getKey();
        boolean walkingFromInclusive = fromBoundPair.getValue();
        walkingRange = createRange(walkingFromBound, walkingFromInclusive, range.getTo(), range.isToInclusive(), range);
        return walkingRange;
    }

    @Override
    public boolean isRangeOver() {
        return !range.isIntersected(walkingRange);
    }

    protected abstract EventRange<T> createRange(T fromBound, boolean fromInclusive, T toBound, boolean toInclusive, EventRange<T> initialRange);
}
