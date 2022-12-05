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

package io.nosqlbench.engine.core.lifecycle;

import io.nosqlbench.api.engine.activityimpl.ActivityDef;
import io.nosqlbench.engine.api.activityapi.core.Activity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.graalvm.polyglot.Value;

import java.security.InvalidParameterException;
import java.util.Map;

public class PolyglotScenarioController {

    private static final Logger logger = LogManager.getLogger("SCENARIO/POLYGLOT");

    private final ScenarioController controller;

    public PolyglotScenarioController(ScenarioController inner) {
        this.controller = inner;
    }

    // for graal polyglot interop
//    public synchronized void run(Value vtimeout, Value vspec) {
//        int timeout = vtimeout.asInt();
//        run(timeout, vspec);
//    }
//
//    public synchronized void run(Map<String,String> map) {
//        controller.run(map);
//    }
//
//    public synchronized void run(int timeout, Map<String,String> map) {
//        controller.run(timeout, map);
//    }
//

    public synchronized Activity run(Object o) {
        if (o instanceof Value) {
            return runValue((Value) o);
        } else if (o instanceof Map) {
            return controller.run((Map<String, String>) o);
        } else if (o instanceof String) {
            return controller.run(o.toString());
        } else {
            throw new RuntimeException("Unrecognized type: " + o.getClass().getCanonicalName());
        }
    }

    public synchronized Activity run(int timeout, Object o) {
        if (o instanceof Value) {
            return runValue(timeout, (Value) o);
        } else if (o instanceof Map) {
            return controller.run(timeout, (Map<String, String>) o);
        } else if (o instanceof String) {
            return controller.run(timeout, o.toString());
        } else {
            throw new RuntimeException("Unrecognized type: " + o.getClass().getCanonicalName());
        }
    }

    private synchronized Activity runValue(Value v) {
        return runValue(Integer.MAX_VALUE, v);
    }

    private synchronized Activity runValue(int timeout, Value spec) {
        logger.debug("run(Value) called with:" + spec);
        if (spec.isHostObject()) {
            return controller.run(timeout, (ActivityDef) spec.asHostObject());
        } else if (spec.isString()) {
            return controller.run(timeout, spec.asString());
        } else if (spec.hasMembers()) {
            return controller.run(timeout, spec.as(Map.class));
        } else if (spec.isHostObject()) {
            Object o = spec.asHostObject();
            if (o instanceof ActivityDef) {
                return controller.run(timeout, (ActivityDef) o);
            } else {
                throw new RuntimeException("unrecognized polyglot host object type for run: " + spec);
            }
        } else {
            throw new RuntimeException("unrecognized polyglot base type for run: " + spec);
        }
    }


    public synchronized Activity start(Object o) {
        if (o instanceof Value) {
            return startValue((Value) o);
        } else if (o instanceof Map) {
            return controller.start((Map<String, String>) o);
        } else if (o instanceof String) {
            return controller.start(o.toString());
        } else {
            throw new RuntimeException("unrecognized type " + o.getClass().getCanonicalName());
        }
    }

    private synchronized Activity startValue(Value spec) {
        if (spec.isHostObject()) {
            return controller.start((ActivityDef) spec.asHostObject());
        } else if (spec.isString()) {
            return controller.start(spec.asString());
        } else if (spec.hasMembers()) {
            return controller.start(spec.as(Map.class));
        } else {
            throw new RuntimeException("unknown base type for graal polyglot: " + spec);
        }
    }

    public synchronized void stop(Object o) {
        if (o instanceof Value) {
            stopValue((Value) o);
        } else if (o instanceof Map) {
            controller.stop((Map<String, String>) o);
        } else if (o instanceof String) {
            controller.stop(o.toString());
        } else {
            throw new RuntimeException("unknown type " + o.getClass().getCanonicalName());
        }
    }

    private synchronized void stopValue(Value spec) {
        if (spec.isHostObject()) {
            controller.stop((ActivityDef) spec.asHostObject());
        } else if (spec.isString()) {
            controller.stop(spec.asString());
        } else if (spec.hasMembers()) {
            controller.stop(spec.as(Map.class));
        } else {
            throw new RuntimeException("unknown base type for graal polyglot: " + spec);
        }
    }

    public synchronized void apply(Object o) {
        if (o instanceof Value) {
            applyValue((Value) o);
        } else if (o instanceof Map) {
            controller.apply((Map<String, String>) o);
        } else {
            throw new RuntimeException("unknown type: " + o.getClass().getCanonicalName());
        }
    }

    private synchronized void applyValue(Value spec) {
        Map<String, String> map = spec.as(Map.class);
        controller.apply(map);
    }

    public synchronized void awaitActivity(Object o) {
        this.await(o);
    }
    public synchronized void await(Object o) {
        if (o instanceof String) {
            controller.await(o.toString());
        } else if (o instanceof Value) {
            awaitValue((Value) o);
        } else if (o instanceof Map) {
            controller.await((Map<String, String>) o);
        } else {
            throw new RuntimeException("unknown type: " + o.getClass().getCanonicalName());
        }
    }

    private synchronized void awaitValue(Value spec) {
        if (spec.isHostObject()) {
            controller.await((ActivityDef) spec.asHostObject());
        } else if (spec.hasMembers()) {
            controller.await(spec.as(Map.class));
        } else if (spec.isString()) {
            controller.await(spec.asString());
        } else {
            throw new RuntimeException("unable to map type for await from polyglot value: " + spec);
        }
    }

    public synchronized void waitMillis(Object o) {
        if (o instanceof Value) {
            waitMillisValue((Value) o);
        } else if (o instanceof Integer) {
            controller.waitMillis((Integer) o);
        } else if (o instanceof Long) {
            controller.waitMillis((Long) o);
        } else if (o instanceof String) {
            controller.waitMillis(Long.parseLong((String)o));
        } else {
            throw new RuntimeException("unknown type: " + o.getClass().getCanonicalName());
        }
    }

    private synchronized void waitMillisValue(Value spec) {
        if (spec.isString()) {
            controller.waitMillis(Long.parseLong(spec.asString()));
        } else if (spec.isNumber()) {
            controller.waitMillis(spec.asLong());
        } else {
            throw new InvalidParameterException(
                "unable to convert polyglot type " + spec + " to a long for waitMillis");
        }
    }

    public synchronized boolean isRunningActivity(Object o) {
        if (o instanceof Value) {
            return isRunningActivityValue((Value) o);
        } else if (o instanceof String) {
            return controller.isRunningActivity(o.toString());
        } else if (o instanceof Map) {
            return controller.isRunningActivity((Map<String, String>) o);
        } else {
            throw new RuntimeException("unknown type:" + o.getClass().getCanonicalName());
        }
    }

    private synchronized boolean isRunningActivityValue(Value spec) {
        if (spec.isHostObject()) {
            return controller.isRunningActivity((ActivityDef) spec.asHostObject());
        } else if (spec.isString()) {
            return controller.isRunningActivity(spec.asString());
        } else if (spec.hasMembers()) {
            return controller.isRunningActivity(spec.as(Map.class));
        } else {
            throw new InvalidParameterException("unable to map type for isRunningActivity from polyglot value: " + spec);
        }
    }

}
