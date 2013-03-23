import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.persistit.util.ArgParser;

public class SphinxDocPrep {

    private final static Pattern PERSISTIT_PATTERN = Pattern
            .compile("(``)?(com\\.persistit(?:\\.[a-z]\\w*)*(?:\\.[A-Z]\\w*)+)(?:#(\\w+(?:[\\(\\)\\,a-zA-Z]*)))?(``)?");

    private final static String[] ARG_TEMPLATE = { "in|string:|Input file", "out|string:|Output file",
            "index|string:|Pathname of index-all.html file", "base|string:|Base of generated URLs", };

    enum BlockState {
        OUT, WAIT_FIRST_BLANK_LINE, WAIT_SECOND_BLANK_LINE
    }

    private AsciiDocIndex index;
    private BlockState block = BlockState.OUT;
    private PrintWriter writer;
    private String base;
    private String indexPath;

    private void prepare(final String[] args) throws Exception {
        final ArgParser ap = new ArgParser("SphinxDocPrep", args, ARG_TEMPLATE);
        final String inPath = ap.getStringValue("in");
        final String outPath = ap.getStringValue("out");

        writer = outPath.isEmpty() ? new PrintWriter(System.out) : new PrintWriter(new FileWriter(outPath));

        base = ap.getStringValue("base");
        if (base.isEmpty()) {
            base = "http://akiban.com/persistit/doc/apidocs";
        }
        indexPath = ap.getStringValue("index");
        if (indexPath.isEmpty()) {
            indexPath = "/home/peter/website/apidocs/index-all.html";
        }

        index = new AsciiDocIndex();
        System.out.print("Building JavaDoc index..");
        index.buildIndex(indexPath, base);
        System.out.println("done");

        processFile(new File(inPath), 0);
        writer.close();
    }

    public void processFile(final File file, final int level) throws Exception {
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        System.out.print("Processing file " + file);
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("@")) {
                processFile(new File(file.getParentFile(), line.substring(1)), level + 1);
            } else {
                processLine(line);
            }
        }
        writer.println();
        System.out.println(" - done");
    }

    private void processLine(final String line) throws Exception {
        if (line.contains("{{bug-list}}")) {
            prepareBugList();
            return;
        } else if (line.contains(".. code-block:") || line.endsWith("::")) {
            block = BlockState.WAIT_FIRST_BLANK_LINE;
        } else if (line.isEmpty()) {
            switch (block) {
            case WAIT_FIRST_BLANK_LINE:
                block = BlockState.WAIT_SECOND_BLANK_LINE;
                break;

            case WAIT_SECOND_BLANK_LINE:
                block = BlockState.OUT;
                break;

            default:
                // no change
            }
        }

        if (block == BlockState.OUT) {
            final StringBuffer sb = new StringBuffer();
            if (line.startsWith("=")) {
                sb.append("=");
            }
            final Matcher matcher = PERSISTIT_PATTERN.matcher(line);
            while (matcher.find()) {
                processMatch(matcher, sb);
            }
            matcher.appendTail(sb);
            writer.println(sb.toString());
        } else {
            writer.println(line);
            writer.flush();
        }
    }

    private void processMatch(final Matcher matcher, final StringBuffer sb) {
        final String className = matcher.group(2);
        final String methodName = matcher.group(3);

        String replacement;
        if (methodName == null) {
            final String url = index.getClassMap().get(className);
            if (url == null || url.isEmpty()) {
                replacement = "<<<Missing class: " + className + ">>>";
            } else {
                replacement = "`" + className + " <" + url + ">`_";
            }
        } else {
            final String from = className + "#" + methodName.split("\\(")[0];
            final SortedMap<String, String> map = index.getMethodMap().tailMap(from);
            String url;
            if (map.isEmpty()) {
                replacement = "<<<Missing method: " + methodName + ">>>";
            } else {
                final String first = map.firstKey();
                if (!first.startsWith(from)) {
                    replacement = "<<<Missing method: " + methodName + ">>>";
                }
                url = map.get(first);
                url = url.replace(" ", "%20");
                String text = first.split("#")[1];
                if (!from.contains("(")) {
                    text = text.split("\\(")[0];
                }
                text = text.replace("com.persistit.encoding.", "");
                text = text.replace("com.persistit.exception.", "");
                text = text.replace("com.persistit.logging.", "");
                text = text.replace("com.persistit.mxbeans.", "");
                text = text.replace("com.persistit.ref.", "");
                text = text.replace("com.persistit.ui.", "");
                text = text.replace("com.persistit.", "");
                text = text.replace("java.lang.", "");
                text = text.replace("java.util.", "");
                replacement = "`" + text + " <" + url + ">`_";
            }
        }

        matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }

    private void prepareBugList() throws IOException {
        final List<String[]> rows = new ArrayList<String[]>();
        rows.add(new String[] { "Bug Reference", "Fixed in|Version", "Summary" });
        BufferedReader reader = null;
        try {
            String urls = "";
            String version = "";
            reader = new BufferedReader(new FileReader("../BugList"));
            final StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    if (sb.length() > 0) {
                        sb.append('|');
                    }
                } else if (Character.isDigit(line.charAt(0))) {
                    if (!urls.isEmpty()) {
                        rows.add(new String[] { urls, version, sb.toString() });
                        urls = "";
                        version = "";
                        sb.setLength(0);
                    }
                    final String[] split = line.split("\\:");
                    for (final String bug : split[0].trim().split(",")) {
                        if (urls.length() > 0) {
                            urls += '|';
                        }
                        urls = urls + "https://launchpad.net/bugs/" + bug;
                    }
                    version = split[1].trim();
                } else {
                    sb.append(line.trim());
                    if (sb.length() > 0) {
                        sb.append('|');
                    }
                }
            }
            if (!urls.isEmpty()) {
                rows.add(new String[] { urls, version, sb.toString() });
            }

            final int[] maxWidth = new int[3];
            for (final String[] row : rows) {
                for (int i = 0; i < 3; i++) {
                    for (final String s : row[i].split("\\|")) {
                        maxWidth[i] = Math.max(s.length(), maxWidth[i]);
                    }
                }
            }

            bugTableLine(sb, false, maxWidth);
            for (int l = 0;; l++) {
                if (bugTableText(sb, rows.get(0), maxWidth, l)) {
                    break;
                }
            }
            bugTableLine(sb, true, maxWidth);
            for (int i = 1; i < rows.size(); i++) {
                final String[] text = rows.get(i);
                for (int l = 0;; l++) {
                    if (bugTableText(sb, text, maxWidth, l)) {
                        break;
                    }
                }
                bugTableLine(sb, false, maxWidth);
            }
        } catch (final IOException e) {
            System.out.println(e + " while trying to read BugList");
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    private void bugTableLine(final StringBuilder sb, final boolean dline, final int[] width) {
        sb.setLength(0);
        sb.append('+');
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < width[j] + 2; i++) {
                sb.append(dline ? '=' : '-');
            }
            sb.append('+');
        }
        writer.println(sb);
        writer.flush();
    }

    private boolean bugTableText(final StringBuilder sb, final String[] text, final int[] width, final int line) {
        final String[] s = new String[3];
        boolean done = true;
        for (int j = 0; j < 3; j++) {
            final String[] split = text[j].split("\\|");
            if (split.length > line) {
                done = false;
                s[j] = split[line];
            }
        }
        if (!done) {
            sb.setLength(0);
            sb.append("|");
            for (int j = 0; j < 3; j++) {
                if (s[j] == null) {
                    s[j] = "";
                }
                sb.append(' ');
                sb.append(s[j]);
                for (int i = s[j].length() + 1; i < width[j] + 2; i++) {
                    sb.append(' ');
                }
                sb.append('|');
            }
            writer.println(sb);
        }
        writer.flush();
        return done;
    }

    public static void main(final String[] args) throws Exception {
        new SphinxDocPrep().prepare(args);
    }
}
