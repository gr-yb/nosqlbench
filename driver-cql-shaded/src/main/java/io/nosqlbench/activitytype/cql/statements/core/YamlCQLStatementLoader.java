package io.nosqlbench.activitytype.cql.statements.core;

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


import io.nosqlbench.engine.api.activityconfig.rawyaml.RawStmtsLoader;
import io.nosqlbench.engine.api.activityimpl.ActivityInitializationError;
import io.nosqlbench.nb.api.content.Content;
import io.nosqlbench.nb.api.content.NBIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings("ALL")
public class YamlCQLStatementLoader {

    private final static Logger logger = LogManager.getLogger(YamlCQLStatementLoader.class);
    List<Function<String, String>> transformers = new ArrayList<>();

    public YamlCQLStatementLoader() {
    }

    public YamlCQLStatementLoader(Function<String, String>... transformers) {
        this.transformers.addAll(Arrays.asList(transformers));
    }

    public AvailableCQLStatements load(String fromPath, String... searchPaths) {

        Content<?> yamlContent = NBIO.all().prefix(searchPaths).name(fromPath).extension(RawStmtsLoader.YAML_EXTENSIONS).one();
        String data = yamlContent.asString();

        for (Function<String, String> xform : transformers) {
            try {
                logger.debug("Applying string transformer to yaml data:" + xform);
                data = xform.apply(data);
            } catch (Exception e) {
                RuntimeException t = new ActivityInitializationError("Error applying string transform to input", e);
                throw t;
            }
        }

        Yaml yaml = getCustomYaml();

        try {
            Iterable<Object> objects = yaml.loadAll(data);
            List<TaggedCQLStatementDefs> stmtListList = new ArrayList<>();
            for (Object object : objects) {
                TaggedCQLStatementDefs tsd = (TaggedCQLStatementDefs) object;
                stmtListList.add(tsd);
            }
            return new AvailableCQLStatements(stmtListList);

        } catch (Exception e) {
            logger.error("Error loading yaml from " + fromPath, e);
            throw e;
        }

    }

    private Yaml getCustomYaml() {
        Constructor constructor = new Constructor(TaggedCQLStatementDefs.class);
        TypeDescription tds = new TypeDescription(TaggedCQLStatementDefs.class);
        tds.putListPropertyType("statements", CQLStatementDef.class);
        constructor.addTypeDescription(tds);
        return new Yaml(constructor);
    }

}
