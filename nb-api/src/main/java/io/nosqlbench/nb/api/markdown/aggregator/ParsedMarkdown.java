package io.nosqlbench.nb.api.markdown.aggregator;

import com.vladsch.flexmark.ext.yaml.front.matter.AbstractYamlFrontMatterVisitor;
import com.vladsch.flexmark.util.ast.Document;
import io.nosqlbench.nb.api.content.Content;
import io.nosqlbench.nb.api.markdown.FlexParser;
import io.nosqlbench.nb.api.markdown.types.FrontMatterInfo;
import io.nosqlbench.nb.api.markdown.types.HasDiagnostics;
import io.nosqlbench.nb.api.markdown.types.MarkdownInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * TODO: Make this a value type
 */
public class ParsedMarkdown implements MarkdownInfo, HasDiagnostics {
    private final static Logger logger = LogManager.getLogger(MarkdownDocs.class);

    private final ParsedFrontMatter frontMatter;
    private final Content<?> content;

    public ParsedMarkdown(Content<?> content) {
        String rawMarkdown = content.asString();
        AbstractYamlFrontMatterVisitor v = new AbstractYamlFrontMatterVisitor();
        Document parsed = FlexParser.parser.parse(rawMarkdown);
        v.visit(parsed);
        Map<String, List<String>> data = v.getData();
        frontMatter = new ParsedFrontMatter(data);
        this.content = content;
        logger.debug("created " + this.toString());
    }

    @Override
    public Path getPath() {
        return content.asPath();
    }

    @Override
    public String getBody() {
        return null;
    }

    @Override
    public FrontMatterInfo getFrontmatter() {
        return frontMatter;
    }

    /**
     * Get a list of diagnostic warnings that might help users know of issues in their
     * markdown content before publication.
     * @param buffer A buffer object, for accumulating many lines of detail, if necessary.
     * @return The buffer, with possible additions
     */
    @Override
    public List<String> getDiagnostics(List<String> buffer) {
        List<String> diagnostics = frontMatter.getDiagnostics();
        if (diagnostics.size()==0) {
            return List.of();
        }
        String[] diags = diagnostics.stream().map(s -> " " + s).toArray(String[]::new);
        buffer.add("found " + diagnostics.size() + " diagnostics for " + getPath().toString());
        buffer.addAll(Arrays.asList(diags));
        return buffer;
    }

    /**
     * The buffer-less version of {@link #getDiagnostics(List)}
     * @return a list of diagnostics lines, zero if there are none
     */
    @Override
    public List<String> getDiagnostics() {
        return getDiagnostics(new ArrayList<>());
    }

    @Override
    public boolean hasAggregations() {
        return getFrontmatter().getAggregations().size()>0;
    }
}
