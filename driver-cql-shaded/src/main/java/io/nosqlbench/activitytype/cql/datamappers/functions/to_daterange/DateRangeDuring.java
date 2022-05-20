package io.nosqlbench.activitytype.cql.datamappers.functions.to_daterange;

/*
 * Copyright (c) 2022 nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.datastax.driver.dse.search.DateRange;
import io.nosqlbench.virtdata.api.annotations.Categories;
import io.nosqlbench.virtdata.api.annotations.Category;
import io.nosqlbench.virtdata.api.annotations.Example;
import io.nosqlbench.virtdata.api.annotations.ThreadSafeMapper;

import java.util.Date;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * Takes an input as a reference point in epoch time, and converts it to a DateRange,
 * with the bounds set to the lower and upper timestamps which align to the
 * specified precision. You can use any of these precisions to control the bounds
 * around the provided timestamp: millisecond, second, minute, hour, day, month, or year.
 */
@ThreadSafeMapper
@Categories(Category.datetime)
public class DateRangeDuring implements LongFunction<DateRange> {

    private final com.datastax.driver.dse.search.DateRange.DateRangeBound.Precision precision;

    @Example({"DateRangeDuring('millisecond')}","Convert the incoming millisecond to an equivalent DateRange"})
    @Example({"DateRangeDuring('minute')}","Convert the incoming millisecond to a DateRange for the minute in which the " +
        "millisecond falls"})
    public DateRangeDuring(String precision) {
        this.precision = com.datastax.driver.dse.search.DateRange.DateRangeBound.Precision.valueOf(precision.toUpperCase());
    }

    @Override
    public DateRange apply(long value) {
        Date date = new Date(value);
        com.datastax.driver.dse.search.DateRange.DateRangeBound lower = com.datastax.driver.dse.search.DateRange.DateRangeBound.lowerBound(date, precision);
        com.datastax.driver.dse.search.DateRange.DateRangeBound upper = com.datastax.driver.dse.search.DateRange.DateRangeBound.upperBound(date, precision);
        com.datastax.driver.dse.search.DateRange dateRange = new com.datastax.driver.dse.search.DateRange(lower, upper);
        return dateRange;
    }
}
