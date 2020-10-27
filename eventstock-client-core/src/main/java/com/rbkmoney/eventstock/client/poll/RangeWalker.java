package com.rbkmoney.eventstock.client.poll;

import com.rbkmoney.eventstock.client.EventRange;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Uses {@link EventRange} to walk through events set.
 */
interface RangeWalker<T extends Comparable, R extends EventRange<T>> {
    /**
     * @return initial range for this walker
     */
    R getRange();

    /**
     * @return current range for this walker which is inside the initial range or null if range is over.
     */
    R getWalkingRange();


    /**
     * Changes current range to new one. Result value is not checked and it's up to developer to use it correctly.
     *
     * @param function returns new current range value.
     * @return moved range, which is equal to {@link #getWalkingRange()} result or null if range is over.
     */
    R setRange(Function<RangeWalker<T, R>, R> function);

    /**
     * Changes one bound in range to value returned by referred function. Direction of change (up or down bound) depends on implementation.
     *
     * @param function returns new bound value based on current from and to values.
     * @return moved range, which is equal to {@link #getWalkingRange()} result or null if range is over.
     */
    R moveRange(BiFunction<RangeWalker<T, R>, Boolean, Map.Entry<T, Boolean>> function);

    /**
     * @return true - if current range is out of initial range bounds; false - otherwise.
     */
    boolean isRangeOver();

}
