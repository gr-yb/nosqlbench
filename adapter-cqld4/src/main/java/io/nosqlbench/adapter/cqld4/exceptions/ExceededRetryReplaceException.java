/*
 * Copyright (c) 2022 nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.adapter.cqld4.exceptions;


import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * This is a synthetic error generated by the cql driver in NoSQLBench when
 * the retryreplace option is used but the number of LWT round-trips from the driver
 * is excessive. The number of LWT round trips allowed is controlled by the
 * maxlwtretries op field.
 */
public class ExceededRetryReplaceException extends CqlGenericCycleException {

    private final AsyncResultSet resultSet;
    private final String queryString;
    private final int retries;

    public ExceededRetryReplaceException(AsyncResultSet resultSet, String queryString, int retries) {
        super("After " + retries + " retries using the retryreplace option, Operation was not applied:" + queryString);
        this.retries = retries;
        this.resultSet = resultSet;
        this.queryString = queryString;
    }

    public AsyncResultSet getResultSet() {
        return resultSet;
    }
    public String getQueryString() { return queryString; }
}
