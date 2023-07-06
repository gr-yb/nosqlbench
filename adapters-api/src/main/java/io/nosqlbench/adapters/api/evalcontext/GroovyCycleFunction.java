/*
 * Copyright (c) 2023 nosqlbench
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

package io.nosqlbench.adapters.api.evalcontext;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import io.nosqlbench.adapters.api.activityimpl.BaseOpDispenser;
import io.nosqlbench.virtdata.core.bindings.Bindings;
import io.nosqlbench.virtdata.core.bindings.BindingsTemplate;
import io.nosqlbench.virtdata.core.templates.ParsedTemplateString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.ArrayList;
import java.util.List;

public abstract class GroovyCycleFunction<T> implements CycleFunction<T> {
    private final static Logger logger = LogManager.getLogger(GroovyBooleanCycleFunction.class);
    protected final Script script; // Groovy Script
    protected final Binding binding; // Groovy binding layer
    protected final Bindings bindings; // NB bindings
    protected String originalExpression;

    public GroovyCycleFunction(ParsedTemplateString template, List<String> imports) {
        this.originalExpression = template.getStmt();

        String scriptBodyWithRawVarRefs = template.getPositionalStatement();
        CompilerConfiguration compilerConfiguration = new CompilerConfiguration();

        // add classes which are in the imports to the groovy evaluation context
        String[] verifiedClasses = expandClassNames(imports);
        ImportCustomizer importer = new ImportCustomizer();
        importer.addImports(verifiedClasses);
        compilerConfiguration.addCompilationCustomizers(importer);

        // scripting env variable bindings
        this.binding = new Binding();
        // virtdata bindings to be evaluated at cycle time
        this.bindings = new BindingsTemplate(template.getBindPoints()).resolveBindings();

        GroovyShell gshell = new GroovyShell(binding, compilerConfiguration);
        this.script = gshell.parse(scriptBodyWithRawVarRefs);
    }

    protected String[] expandClassNames(List<String> groovyImportedClasses) {
        ClassLoader loader = BaseOpDispenser.class.getClassLoader();

        List<String> classNames = new ArrayList<>();
        for (String candidateName : groovyImportedClasses) {
            if (candidateName.endsWith(".*")) {
                throw new RuntimeException("You can not use wildcard package imports like '" + candidateName + "'");
            }
            try {
                loader.loadClass(candidateName);
                classNames.add(candidateName);
                logger.debug(() -> "added import " + candidateName);
            } catch (Exception e) {
                throw new RuntimeException("Class '" + candidateName + "' was not found for groovy imports.");
            }
        }
        return classNames.toArray(new String[0]);
    }

    @Override
    public String getExpressionDetails() {
        return this.originalExpression;
    }


    @Override
    public <V> void setVariable(String name, V value) {
        this.binding.setVariable(name, value);
    }

    @Override
    public abstract T apply(long value);
}
