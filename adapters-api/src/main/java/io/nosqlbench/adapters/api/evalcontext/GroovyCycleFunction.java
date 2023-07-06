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
import io.nosqlbench.virtdata.core.templates.BindPoint;
import io.nosqlbench.virtdata.core.templates.ParsedTemplateString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.customizers.ImportCustomizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GroovyCycleFunction<T> implements CycleFunction<T> {
    private final static Logger logger = LogManager.getLogger(GroovyBooleanCycleFunction.class);

    protected String originalExpression; // Groovy script as provided
    protected final Script script; // Groovy Script as compiled
    protected final Binding variableBindings; // Groovy binding layer
    protected final Bindings bindingFunctions; // NB bindings

    /**
     * Instantiate a cycle function from basic types
     * @param scriptText The raw script text, not including any bind point or capture point syntax
     * @param bindingSpecs The names and recipes of bindings which are referenced in the scriptText
     * @param imports The package imports to be installed into the execution environment
     */
    public GroovyCycleFunction(String scriptText, Map<String,String> bindingSpecs, List<String> imports) {
        this.originalExpression=scriptText;

        // scripting env variable bindings
        this.variableBindings = new Binding();

        // virtdata bindings to be evaluated at cycle time
        this.bindingFunctions = new BindingsTemplate().addFieldBindings(bindingSpecs).resolveBindings();

        // add classes which are in the imports to the groovy evaluation context
        String[] verifiedClasses = expandClassNames(imports);

        CompilerConfiguration compilerConfiguration = new CompilerConfiguration();
        ImportCustomizer importer = new ImportCustomizer().addImports(verifiedClasses);
        compilerConfiguration.addCompilationCustomizers(importer);

        GroovyShell gshell = new GroovyShell(variableBindings, compilerConfiguration);
        this.script = gshell.parse(scriptText);
    }

    public GroovyCycleFunction(ParsedTemplateString template, List<String> imports) {
        this(
            template.getPositionalStatement(),
            resolveBindings(template.getBindPoints()),
            imports
        );
    }

    private GroovyCycleFunction(Script script, Supplier<Binding> bindingSource, Bindings bindings) {
        this.script = script;
        this.bindingFunctions = bindings;
        this.variableBindings = bindingSource.get();
    }

    private static Map<String, String> resolveBindings(List<BindPoint> bindPoints) {
        return new BindingsTemplate(bindPoints).getMap();
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
        this.variableBindings.setVariable(name, value);
    }

    @Override
    public T apply(long value) {
        Map<String, Object> values = bindingFunctions.getAllMap(value);
        values.forEach((k,v)-> variableBindings.setVariable(k,v));
        T result= (T) script.run();
        return result;
    }

    @Override
    public CycleFunction<T> newInstance() {
        return new GroovyCycleFunction<T>(this.script,() -> new Binding(),this.bindingFunctions);
    }
}
