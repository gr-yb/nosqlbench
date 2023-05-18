/*
 * Copyright (c) 2022-2023 nosqlbench
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

package io.nosqlbench.api.errors;

import java.io.Serializable;

public class ExpectedResultVerificationError extends RuntimeException {
    private final int triesLeft;
    private final Serializable expectedResultExpression;

    public ExpectedResultVerificationError(int triesLeft, Serializable expectedResultExpression) {
        this.triesLeft = triesLeft;
        this.expectedResultExpression = expectedResultExpression;
    }

    public int getTriesLeft() {
        return triesLeft;
    }

    public Serializable getExpectedResultExpression() {
        return expectedResultExpression;
    }

}
