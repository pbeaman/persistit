/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

package com.persistit;

import static com.persistit.Configuration.APPEND_ONLY_PROPERTY_NAME;
import static com.persistit.Configuration.BUFFERS_PROPERTY_NAME;
import static com.persistit.Configuration.ENABLE_JMX_PROPERTY_NAME;
import static com.persistit.Configuration.JOURNAL_PATH_PROPERTY_NAME;
import static com.persistit.Configuration.RMI_REGISTRY_PORT_PROPERTY_NAME;
import static com.persistit.Configuration.VOLUME_PROPERTY_PREFIX;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.persistit.JournalManager.PageNode;
import com.persistit.JournalManager.TransactionMapItem;
import com.persistit.JournalManager.TreeDescriptor;
import com.persistit.JournalRecord.CP;
import com.persistit.JournalRecord.JH;
import com.persistit.exception.PersistitException;
import com.persistit.mxbeans.JournalManagerMXBean;
import com.persistit.util.ArgParser;
import com.persistit.util.Util;

/**
 * <p>
 * Handle commands delivered interactively as command strings. For example, the
 * following loads a Persistit database located in directory /var/lib/data,
 * selects a volume named xyz, and displays page 42 of that volume:
 * 
 * <code><pre>
 * load datapath=/var/lib/data
 * select volume=xyz
 * view page=42
 * </pre></code>
 * </p>
 * <p>
 * CLI operates in one of two modes: "live" and "standalone".
 * </p>
 * <p>
 * In live mode you use one of several access methods to invoke commands to be
 * processed within an actively running server process that has initialized
 * Persistit. Access methods include the AdminUI utility, the Persistit
 * management MBean, a simple network server that can receive command lines from
 * an external tool, or via an application program using the {@link Management}
 * interface.
 * </p>
 * <p>
 * In stand-alone mode you specify through the <code>open</code> command a path
 * on which to find Persistit database files; the open command creates a
 * read-only version Persistit instance capable of performing read-only
 * operations such as <code>save</code>, <code>backup</code> or
 * <code>icheck</code> on the data found in those files.
 * </p>
 * <p>
 * CLI can set up an extremely simple network server that works with telnet,
 * curl or the simple client built into the {@link #main(String[])} method of
 * this class. The advantage is that you can then issue commands interactively
 * from a simple network client using the full facilities of the shell,
 * including piping the output to tools such as grep and more.
 * </p>
 * <p>
 * To run the CLI in standalone mode, simply execute <blockquote><code><pre>
 * java -cp persisit.jar com.persistit.Persistit cliport=9999
 * </pre></code></blockquote> or <blockquote><code><pre>
 * java -cp persisit.jar com.persistit.Persistit script=pathname
 * </pre></code></blockquote> where persistit.jar contains the Persistit
 * library. The first option specifies a port on which CLI will listen for
 * commands. (Use a port number larger than 1024 to avoid permissions problems.)
 * The second option executes commands from a text file in batch mode.
 * </p>
 * <p>
 * The following *nix client script works with the network server facility:
 * <blockquote><code><pre>
 *   #!/bin/sh
 *   java -cp persistit.jar com.persistit.CLI 9999 $*
 * </pre></code></blockquote> (Substitute the port number assigned to the
 * cliport parameter above.)
 * </p>
 * <p>
 * With this script you can then enter command such as this at the shell:
 * <blockquote><code><pre>
 * pcli init datapath=/var/lib/data
 * pcli select volume=xyz
 * pcli view page=42
 * </pre></code></blockquote>
 * </p>
 * <p>
 * Commands are defined below in methods annotated with @Cmd having parameters
 * annotated with @Arg. The format of the argument annotation is specified in
 * {@link ArgParser}. Enter the command 'help' to see a readable list of
 * commands and their arguments.
 * </p>
 * 
 * @author peter
 */

public class CLI {
    private final static int BUFFER_SIZE = 1024 * 1024;
    /*
     * "Huge" block size for pseudo-journal created by dump command.
     */
    private final static long HUGE_BLOCK_SIZE = 1000L * 1000L * 1000L * 1000L;
    private final static char DEFAULT_COMMAND_DELIMITER = ' ';
    private final static char DEFAULT_QUOTE = '\\';
    private final static int MAX_PAGE_NODES = 10000;

    private final static Map<String, Command> COMMANDS = new TreeMap<String, Command>();

    private final static Class<?>[] CLASSES = { CLI.class, BackupTask.class, IntegrityCheck.class, StreamSaver.class,
            StreamLoader.class, StatisticsTask.class, TaskCheck.class, VolumeHeader.class };

    static {
        for (final Class<?> clazz : CLASSES) {
            registerCommands(clazz);
        }
    }

    /**
     * Registers command methods provided by the supplied Class. To be
     * registered as a CLI command, a method must be identified by a @Cmd
     * annotation, and its arguments must be defined with @Arg annotations. This
     * method allows applications to extend the CLI.
     * 
     * @param clazz
     */
    public static void registerCommands(final Class<?> clazz) {
        for (final Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Cmd.class)) {
                final String name = method.getAnnotation(Cmd.class).value();
                final Annotation[][] parameters = method.getParameterAnnotations();
                final String[] argTemplate = new String[parameters.length];
                int index = 0;
                for (final Annotation[] annotations : parameters) {
                    final Arg argAnnotation = (Arg) annotations[0];
                    argTemplate[index++] = argAnnotation.value();
                }
                COMMANDS.put(name, new Command(name, argTemplate, method));
            }
        }
    }

    /**
     * Annotation for methods that implement commands
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Cmd {
        String value();
    }

    /**
     * Annotation for parameters of command methods
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    public @interface Arg {
        String value();
    }

    private final static Pattern ALL = Pattern.compile(".*");

    /**
     * Simple client for CLI server. Run the command
     * 
     * <pre>
     * <code>
     * java -cp <i>classpath</i> com.persistit.CLI <i>port command arg arg arg ...</i>
     * </code>
     * </pre>
     * 
     * to execute a CLI command on the CLI server, e.g.,
     * 
     * <pre>
     * <code>
     * java -cp persistit.jar com.persistit.CLI 9999 select volume=akiban_data
     * </code>
     * </pre>
     * 
     * To execute the select command on a server on port 9999.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        final StringBuilder sb = new StringBuilder();
        int port = -1;
        String host = null;

        final String[] hostPieces = args[0].split(":");
        switch (hostPieces.length) {
        case 1:
            port = Integer.parseInt(hostPieces[0]);
            break;
        case 2:
            host = hostPieces[0];
            port = Integer.parseInt(hostPieces[1]);
            break;
        }

        for (int index = 1; index < args.length; index++) {
            if (index > 1) {
                sb.append(' ');
            }
            sb.append(args[index]);
        }
        sb.append('\n');

        if (port == -1) {
            throw new IllegalArgumentException("Invalid host or port specified by " + args[0]);
        }

        final Socket socket = new Socket(host, port);
        final OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream());
        writer.write(sb.toString());
        writer.flush();
        final InputStreamReader reader = new InputStreamReader(socket.getInputStream());
        int c;
        while ((c = reader.read()) != -1) {
            System.out.print((char) c);
        }
        System.out.println();
    }

    /**
     * Invoke lines read from the supplied <code>BufferedReader</code> as CLI
     * commands and write any generated output to the supplied
     * <code>PrintWriter</code>.
     * 
     * @param persistit
     * @param reader
     * @param writer
     * @throws Exception
     */
    public static void runScript(final Persistit persistit, final BufferedReader reader, final PrintWriter writer)
            throws Exception {
        final CLI cli = new CLI(persistit, reader, writer);
        cli.commandLoop();
        cli.close(false);
        writer.println();
        writer.flush();
    }

    private static long availableMemory() {
        final MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long max = mu.getMax();
        if (max == -1) {
            max = mu.getInit();
        }
        return max;
    }

    /**
     * Parse a command line string consisting of a command followed by flags and
     * name=value parameters, all separate by spaces. Argument values containing
     * spaces can be quoted by a leading backslash.
     * 
     * @param commandLine
     * @return List of String values, one per command name or argument token.
     */
    public static List<String> pieces(final String commandLine) {
        final StringBuilder sb = new StringBuilder();
        final List<String> strings = new ArrayList<String>();
        char commandDelimiter = DEFAULT_COMMAND_DELIMITER;
        char quote = DEFAULT_QUOTE;

        boolean quoted = false;
        for (int index = 0; index < commandLine.length(); index++) {
            final char c = commandLine.charAt(index);
            if (index == 0 && !Character.isLetter(c)) {
                commandDelimiter = c;
                continue;
            }
            if (index == 1 && commandDelimiter != DEFAULT_COMMAND_DELIMITER && !Character.isLetter(c)
                    && c != commandDelimiter) {
                quote = c;
                continue;
            }
            if (quoted) {
                sb.append(c);
                quoted = false;
                continue;
            }
            if (c == quote) {
                quoted = true;
                continue;
            }
            if (c == commandDelimiter) {
                if (sb.length() > 0) {
                    strings.add(sb.toString());
                    sb.setLength(0);
                }
                continue;
            }
            sb.append(c);
        }
        if (sb.length() > 0) {
            strings.add(sb.toString());
        }
        return strings;
    }

    static Task parseTask(final Persistit persistit, final String line) throws Exception {
        final List<String> pieces = pieces(line);
        if (pieces.isEmpty()) {
            return null;
        }
        final String commandName = pieces.remove(0);
        final Command command = COMMANDS.get(commandName);
        if (command == null) {
            return null;
        }
        final Task task = command.createTask(persistit,
                new ArgParser(commandName, pieces.toArray(new String[pieces.size()]), command.argTemplate).strict());
        if (task != null) {
            task.setPersistit(persistit);
        }
        return task;
    }

    interface LineReader {
        String readLine() throws IOException;

        PrintWriter writer();

        void close() throws IOException;
    }

    private class NullReader implements LineReader {
        @Override
        public String readLine() throws IOException {
            return null;
        }

        @Override
        public PrintWriter writer() {
            return _writer;
        }

        @Override
        public void close() throws IOException {
        }

    }

    /**
     * Implements the network server facility. Note that readLine() accepts a
     * new connection for each command.
     * 
     * @author peter
     * 
     */
    private static class NetworkReader implements LineReader {

        final ServerSocket serverSocket;
        Socket socket;
        PrintWriter writer;

        private NetworkReader(final int port) throws IOException {
            this.serverSocket = new ServerSocket(port);
        }

        @Override
        public String readLine() throws IOException {
            close();
            socket = serverSocket.accept();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            return reader.readLine();
        }

        @Override
        public PrintWriter writer() {
            return writer;
        }

        @Override
        public void close() throws IOException {
            if (writer != null) {
                writer.close();
                writer = null;
            }
            if (socket != null) {
                socket.close();
                socket = null;
            }
        }
    }

    private static class ScriptReader implements LineReader {

        private final BufferedReader _reader;
        private final PrintWriter _writer;

        private ScriptReader(final BufferedReader reader, final PrintWriter writer) {
            _reader = reader;
            _writer = writer;
        }

        @Override
        public String readLine() throws IOException {
            final String line = _reader.readLine();
            if (line != null) {
                _writer.println();
                _writer.println(">> " + line);
                _writer.flush();
            }
            return line;
        }

        @Override
        public PrintWriter writer() {
            return _writer;
        }

        @Override
        public void close() throws IOException {
            _reader.close();
            _writer.close();
        }

    }

    private static class Command {
        final String name;
        final String[] argTemplate;
        final Method method;

        private Command(final String name, final String[] argTemplate, final Method method) {
            this.name = name;
            this.argTemplate = argTemplate;
            this.method = method;
        }

        private String execute(final CLI cli, final ArgParser ap) throws Exception {
            final Object[] args = invocationArgs(ap);
            if (method.getReturnType() == String.class) {
                final String result = (String) method.invoke(cli, args);
                return result;
            } else if (Task.class.isAssignableFrom(method.getReturnType())) {
                final Task task = (Task) method.invoke(cli, args);
                task.setPersistit(cli._persistit);
                task.setMaximumTime(-1);
                task.setMessageWriter(cli._writer);
                task.runTask();
                task.setPersistit(null);
                return task.getStatus();
            } else {
                throw new IllegalStateException(this + " must return either a Task or a String");
            }
        }

        private Task createTask(final Persistit persistit, final ArgParser ap) throws Exception {
            if (Task.class.isAssignableFrom(method.getReturnType())) {
                final CLI cli = persistit.getSessionCLI();
                final Object[] args = invocationArgs(ap);
                final Task task = (Task) method.invoke(cli, args);
                return task;
            } else {
                return null;
            }
        }

        private Object[] invocationArgs(final ArgParser ap) {
            final Class<?>[] types = method.getParameterTypes();
            final Object[] args = new Object[types.length];
            for (int index = 0; index < types.length; index++) {
                final Class<?> type = types[index];
                if (String.class.equals(type)) {
                    args[index] = ap.stringValue(index);
                } else if (int.class.equals(type)) {
                    args[index] = ap.intValue(index);
                } else if (long.class.equals(type)) {
                    args[index] = ap.longValue(index);
                } else if (boolean.class.equals(type)) {
                    args[index] = ap.booleanValue(index);
                } else {
                    throw new IllegalArgumentException("Method " + method + " takes an unsupported argument type "
                            + type);
                }
            }
            return args;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(name);
            sb.append(Util.NEW_LINE);
            sb.append(new ArgParser(name, new String[0], argTemplate).strict());
            return sb.toString();
        }
    }

    private static class NotOpenException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    // ----------------------

    private LineReader _lineReader;
    PrintWriter _writer = new PrintWriter(System.out);
    private final Stack<BufferedReader> _sourceStack = new Stack<BufferedReader>();
    private Persistit _persistit;
    private boolean _stop = false;
    private Volume _currentVolume;
    private Tree _currentTree;
    private final boolean _live;
    private int _commandCount;
    private String _lastStatus;

    public CLI(final Persistit persistit) {
        _persistit = persistit;
        _live = persistit != null;
        _lineReader = new NullReader();
    }

    public CLI(final Persistit persistit, final int port) throws IOException {
        _lineReader = new NetworkReader(port);
        _persistit = persistit;
        _live = persistit != null;
    }

    public CLI(final Persistit persistit, final BufferedReader reader, final PrintWriter writer) {
        _lineReader = new ScriptReader(reader, writer);
        _persistit = persistit;
        _live = persistit != null;
    }

    Volume getCurrentVolume() {
        return _currentVolume;
    }

    Tree getCurrentTree() {
        return _currentTree;
    }

    boolean isLive() {
        return _live;
    }

    int getCommandCount() {
        return _commandCount;
    }

    String getLastStatus() {
        return _lastStatus;
    }

    void setLineReader(final LineReader reader) {
        _lineReader = reader;
    }

    private void commandLoop() throws Exception {
        while (!_stop) {
            final String input;
            if (!_sourceStack.isEmpty()) {
                input = _sourceStack.peek().readLine();
                if (input == null) {
                    _sourceStack.pop();
                    continue;
                }
            } else {
                input = _lineReader.readLine();
            }
            if (input == null) {
                break;
            }
            _writer = _lineReader.writer();
            _commandCount++;
            _lastStatus = input;
            try {
                final List<String> list = pieces(input);
                if (list.isEmpty()) {
                    continue;
                }
                final String commandName = list.get(0);
                if (commandName.startsWith("#")) {
                    continue;
                }

                if ("exit".equals(commandName) || "quit".equals(commandName)) {
                    _stop = true;
                    close(false);
                    _lastStatus = "Done";
                    _writer.println("Done");
                    _lineReader.close();
                }

                // Handle intrinsic commands
                final Command command = COMMANDS.get(commandName);
                if (command != null) {
                    list.remove(0);
                    try {
                        final String[] args = list.toArray(new String[list.size()]);
                        final ArgParser ap = new ArgParser(commandName, args, command.argTemplate).strict();
                        if (!ap.isUsageOnly()) {
                            final String result = command.execute(this, ap);
                            if (result != null) {
                                _writer.println(result);
                            }
                            _lastStatus += " - done";
                        }
                    } catch (final InvocationTargetException e) {
                        _lastStatus += e.getTargetException();
                        _writer.println(e.getTargetException());
                    } catch (final RuntimeException e) {
                        _lastStatus += e;
                        e.printStackTrace(_writer);
                    } catch (final Exception e) {
                        _lastStatus += e;
                        _writer.println(e);
                    }
                    continue;
                }
                _lastStatus += " - invalid command";
                _writer.println("No such command " + commandName);
            } finally {
                _writer.flush();
            }
        }
    }

    void checkOpen() throws NotOpenException {
        if (_persistit == null) {
            throw new NotOpenException();
        }
    }

    /**
     * Open files on disk and attempt to make a read-only Persistit instance.
     * This method does not return a Task and cannot be executed in a live
     * 
     * @param datapath
     * @param journalpath
     * @param volumepath
     * @param rmiport
     * @param y
     * @return Result description
     * @throws Exception
     */
    @Cmd("open")
    String open(@Arg("datapath|string|Data path") final String datapath,
            @Arg("journalpath|string|Journal path") final String journalpath,
            @Arg("volumepath|string|Volume file") final String volumepath,
            @Arg("rmiport|int:1099:0:99999|RMI Management port") final int rmiport,
            @Arg("_flag|y|Recover committed transactions") final boolean y) throws Exception {

        if (_live) {
            return "Cannot open another Persistit instance within a live system";
        }
        close(false);

        final String jpath = journalPath(filesOnPath(journalpath.isEmpty() ? datapath : journalpath));
        final List<VolumeSpecification> volumeSpecifications = volumeSpecifications(
                filesOnPath(volumepath.isEmpty() ? datapath : volumepath), Long.MAX_VALUE);
        final Set<Integer> bufferSizes = new HashSet<Integer>();
        for (final VolumeSpecification vs : volumeSpecifications) {
            bufferSizes.add(vs.getPageSize());
        }
        final Properties properties = new Properties();
        final long bpoolMemory = availableMemory() / 2;
        for (final Integer size : bufferSizes) {
            final int alloc = (int) (size * 1.25);
            final int count = (int) ((bpoolMemory / bufferSizes.size()) / alloc);
            properties.put(BUFFERS_PROPERTY_NAME + size, Integer.toString(count));
        }
        int index = 0;
        for (final VolumeSpecification vs : volumeSpecifications) {
            String value = vs.toString();
            if (!y) {
                value += ",readOnly";
            }
            properties.put(VOLUME_PROPERTY_PREFIX + (++index), value);
        }
        if (jpath != null) {
            properties.put(JOURNAL_PATH_PROPERTY_NAME, jpath);
        }
        properties.put(APPEND_ONLY_PROPERTY_NAME, "true");

        if (rmiport > 0) {
            properties.put(RMI_REGISTRY_PORT_PROPERTY_NAME, Integer.toString(rmiport));
        }
        properties.put(ENABLE_JMX_PROPERTY_NAME, "true");

        final Persistit persistit = new Persistit();
        if (!y) {
            persistit.getRecoveryManager().setRecoveryDisabledForTestMode(true);
        }
        persistit.setProperties(properties);
        persistit.initialize();

        /**
         * Following is a hack to figure ought whether there is a classIndex in
         * exactly one volume, and if so, make is the system volume. There
         * should be an API in the Persistit class itself to do this, but
         * currently there isn't one.
         */
        Volume sysvol = null;
        for (final Volume volume : persistit.getVolumes()) {
            if (volume.getTree(ClassIndex.CLASS_INDEX_TREE_NAME, false) != null) {
                if (sysvol == null) {
                    sysvol = volume;
                } else {
                    sysvol = null;
                    break;
                }
            }
        }
        if (sysvol != null) {
            persistit.getConfiguration().setSysVolume(sysvol.getName());
        }

        _persistit = persistit;
        return "Last valid checkpoint=" + persistit.getRecoveryManager().getLastValidCheckpoint().toString();
    }

    @Cmd("close")
    String close(@Arg("_flag|f|Flush modifications to disk") final boolean flush) throws Exception {
        if (_persistit != null) {
            try {
                if (_live) {
                    return "Detaching from live Persistit instance without closing it";
                } else {
                    _persistit.shutdownGUI();
                    _persistit.close(flush);
                }
            } catch (final Exception e) {
                return e.toString();
            } finally {
                _persistit = null;
                _currentVolume = null;
                _currentTree = null;
            }
        }
        return "ok";
    }

    @Cmd("list")
    Task list(final @Arg("trees|string:*|Volume and/or tree specification") String tstring,
            final @Arg("_flag|r|Regular expression") boolean r) throws Exception {
        return new Task() {

            @Override
            public void runTask() throws Exception {
                if (_persistit == null) {
                    postMessage("Persistit not loaded", LOG_NORMAL);
                    return;
                }

                final TreeSelector selector = TreeSelector.parseSelector(tstring, r, '\\');

                final StringBuilder sb = new StringBuilder();
                for (final Volume volume : _persistit.getVolumes()) {
                    if (selector.isVolumeNameSelected(volume.getName())) {
                        sb.append(volume);
                        sb.append(Util.NEW_LINE);
                        for (final String treeName : volume.getTreeNames()) {
                            if (selector.isTreeNameSelected(volume.getName(), treeName)) {
                                final Tree tree = volume.getTree(treeName, false);
                                sb.append("   ");
                                sb.append(tree);
                                sb.append(Util.NEW_LINE);
                            }
                        }
                    }
                }
                postMessage(sb.toString(), LOG_NORMAL);
                return;
            }

            @Override
            public String getStatus() {
                return "";
            }
        };
    }

    @Cmd("jview")
    Task jview(final @Arg("path|string:|Journal file name") String path,
            final @Arg("start|long:0:0:10000000000000|Start journal address") long start,
            final @Arg("end|long:1000000000000000000:0:1000000000000000000|End journal address") long end,
            final @Arg("types|String:*|Selected record types, for example, \"PA,PM,CP\"") String types,
            final @Arg("pages|String:*|Selected pages, for example, \"0,1,200-299,33333-\"") String pages,
            final @Arg("timestamps|String:*|Selected timestamps, for example, \"132466-132499\"") String timestamps,
            final @Arg("maxkey|int:42:4:10000|Maximum displayed key length") int maxkey,
            final @Arg("maxvalue|int:42:4:100000|Maximum displayed value length") int maxvalue,
            final @Arg("_flag|v|Verbose dump - includes PageMap and TransactionMap details") boolean v)
            throws Exception {
        return new Task() {

            @Override
            public void runTask() throws Exception {
                final JournalTool jt = new JournalTool(_persistit);
                jt.setAction(jt.new SimpleDumpAction() {
                    @Override
                    protected void write(final String msg) {
                        postMessage(msg, LOG_NORMAL);
                    }
                });
                jt.init(path, start, end, types, pages, timestamps, maxkey, maxvalue, v);
                jt.setWriter(new PrintWriter(System.out));
                jt.scan();
            }

            @Override
            public String getStatus() {
                return "";
            }
        };
    }

    @Cmd("source")
    Task source(final @Arg("file|string|Read commands from file") String fileName) throws Exception {
        return new Task() {

            @Override
            public void runTask() throws Exception {
                if (!fileName.isEmpty()) {
                    final FileReader in = new FileReader(fileName);
                    _sourceStack.push(new BufferedReader(new BufferedReader(in)));
                    postMessage(String.format("Source is %s", fileName), LOG_NORMAL);
                    return;
                } else {
                    _sourceStack.clear();
                    postMessage("Source is console", LOG_NORMAL);
                    return;
                }
            }

            @Override
            public String getStatus() {
                return "";
            }
        };

    }

    @Cmd("adminui")
    Task adminui(final @Arg("_flag|g|Start") boolean g, final @Arg("_flag|x|Stop") boolean x) throws Exception {
        return new Task() {

            @Override
            public void runTask() throws Exception {

                if (_persistit == null) {
                    postMessage("Persistit not loaded", LOG_NORMAL);
                    return;
                }
                if (g) {
                    _persistit.setupGUI(false);
                    postMessage("Started AdminUI", LOG_NORMAL);
                    return;
                }
                if (x) {
                    _persistit.shutdownGUI();
                    postMessage("Stopped AdminUI", LOG_NORMAL);
                    return;
                }
                postMessage("No action specified", LOG_NORMAL);
                return;
            }

            @Override
            public String getStatus() {
                return "";
            }
        };
    }

    @Cmd("select")
    Task select(final @Arg("tree|string:*|Volume and and/or tree specification") String tstring,
            final @Arg("_flag|r|Regular expression") boolean r) throws Exception {

        return new Task() {

            @Override
            public void runTask() throws Exception {

                _currentTree = null;
                _currentVolume = null;

                if (_persistit == null) {
                    postMessage("Persistit not loaded", LOG_NORMAL);
                    return;
                }

                final List<Object> selected = new ArrayList<Object>();
                final TreeSelector selector = TreeSelector.parseSelector(tstring, r, '\\');

                for (final Volume volume : _persistit.getVolumes()) {
                    if (selector.isVolumeNameSelected(volume.getName())) {
                        if (selector.isVolumeOnlySelection(volume.getName())) {
                            selected.add(volume);
                        } else {
                            for (final String treeName : volume.getTreeNames()) {
                                if (selector.isTreeNameSelected(volume.getName(), treeName)) {
                                    selected.add(volume.getTree(treeName, false));
                                }
                            }
                        }
                    }

                }
                if (selected.isEmpty()) {
                    postMessage("No volumes or trees selected", LOG_NORMAL);
                    return;
                }
                if (selected.size() > 1) {
                    postMessage("Too many volumes or trees selected: " + selected, LOG_NORMAL);
                    return;
                }

                if (selected.get(0) instanceof Volume) {
                    _currentVolume = (Volume) selected.get(0);
                    _currentTree = null;
                    postMessage(String.format("Volume %s selected", _currentVolume), LOG_NORMAL);
                } else {
                    _currentTree = (Tree) selected.get(0);
                    _currentVolume = _currentTree.getVolume();
                    postMessage(String.format("Volume %s tree %s selected", _currentVolume, _currentTree), LOG_NORMAL);
                }

            }

            @Override
            public String getStatus() {
                return "";
            }
        };
    }

    @Cmd("path")
    Task path(final @Arg("key|string|Key") String keyString) throws Exception {

        return new Task() {

            @Override
            public void runTask() throws Exception {
                if (_persistit == null) {
                    postMessage("Persistit not loaded", LOG_NORMAL);
                    return;
                }

                if (_currentVolume == null || _currentTree == null) {
                    postMessage("Tree not selected", LOG_NORMAL);
                    return;
                }
                final Exchange exchange = new Exchange(_currentTree);
                if (!keyString.isEmpty()) {
                    new KeyParser(keyString).parseKey(exchange.getKey());
                }
                final StringBuilder sb = new StringBuilder();
                final int depth = _currentTree.getDepth();
                for (int level = depth; --level >= 0;) {
                    final Buffer copy = exchange.fetchBufferCopy(level);
                    if (sb.length() > 0) {
                        sb.append(Util.NEW_LINE);
                    }
                    sb.append(copy);
                }
                postMessage(sb.toString(), LOG_NORMAL);
                return;
            }

            @Override
            public String getStatus() {
                return "";
            }

        };
    }

    @Cmd("pview")
    Task pview(final @Arg("page|long:-1:-1:99999999999999999|Page address") long pageAddress,
            final @Arg("jaddr|long:-1:-1:99999999999999999|Journal address of a PA page record") long journalAddress,
            final @Arg("index|int:-1:-1:999999999|Buffer pool index") int index,
            final @Arg("pageSize|int:16384:1024:16384|Buffer pool index") int pageSize,
            final @Arg("level|int:0:0:30|Tree level") int level, final @Arg("key|string|Key") String keyString,
            final @Arg("find|long:-1:0:99999999999999999|Optional page pointer to find") long findPointer,
            final @Arg("maxkey|int:42:4:10000|Maximum displayed key length") int maxkey,
            final @Arg("maxvalue|int:42:4:100000|Maximum displayed value length") int maxvalue,
            final @Arg("context|int:3:0:100000|Context lines") int context,
            final @Arg("_flag|a|All lines") boolean allLines, final @Arg("_flag|s|Summary only") boolean summary)
            throws Exception {

        return new Task() {

            @Override
            public void runTask() throws Exception {
                if (_persistit == null) {
                    postMessage("Persistit not loaded", LOG_NORMAL);
                    return;
                }

                final Buffer buffer;
                int specified = 0;
                if (pageAddress >= 0) {
                    specified++;
                }
                if (journalAddress >= 0) {
                    specified++;
                }
                if (index >= 0) {
                    specified++;
                }
                if (!keyString.isEmpty()) {
                    specified++;
                }
                if (specified != 1) {
                    postMessage("Specify one of key=<key>, page=<page address> or journal=<journal address>",
                            LOG_NORMAL);
                    return;
                }
                if (index >= 0) {
                    final BufferPool pool = _persistit.getBufferPool(pageSize);
                    buffer = pool.getBufferCopy(index);
                } else if (journalAddress >= 0) {
                    buffer = _persistit.getJournalManager().readPageBuffer(journalAddress);
                    if (buffer == null) {
                        postMessage(String.format("Journal address %,d is not a valid PA record", journalAddress),
                                LOG_NORMAL);
                        return;
                    }
                    buffer.setValid();
                } else if (pageAddress >= 0) {
                    if (_currentVolume == null) {
                        postMessage("Select a volume", LOG_NORMAL);
                        return;
                    }
                    buffer = _currentVolume.getPool().getBufferCopy(_currentVolume, pageAddress);
                } else {
                    if (_currentTree == null) {
                        postMessage("Select a tree", LOG_NORMAL);
                        return;
                    }
                    final Exchange exchange = new Exchange(_currentTree);

                    if (!keyString.isEmpty()) {
                        new KeyParser(keyString).parseKey(exchange.getKey());
                    }
                    buffer = exchange.fetchBufferCopy(level);
                }
                if (summary) {
                    postMessage(buffer.toString(), LOG_NORMAL);
                    return;
                } else {
                    postMessage(buffer.toStringDetail(findPointer, maxkey, maxvalue, context, allLines), LOG_NORMAL);
                    return;
                }
            }

            @Override
            public String getStatus() {
                return "";
            }
        };
    }

    @Cmd("pviewchain")
    Task pviewchain(final @Arg("page|long:0:0:99999999999999999|Starting page address") long pageAddress,
            final @Arg("find|long:-1:0:99999999999999999|Optional page pointer to find") long findPointer,
            final @Arg("count|int:32:1:1000000|Maximum number of pages to display") long maxcount,
            final @Arg("maxkey|int:42:4:10000|Maximum displayed key length") int maxkey,
            final @Arg("maxvalue|int:42:4:100000|Maximum displayed value length") int maxvalue,
            final @Arg("context|int:3:0:100000|Context lines") int context,
            final @Arg("_flag|a|All lines") boolean allLines, final @Arg("_flag|s|Summary only") boolean summary) {

        return new Task() {

            @Override
            protected void runTask() throws Exception {
                long currentPage = pageAddress;
                int count = 0;
                while (currentPage > 0 && count++ < maxcount) {
                    if (_currentVolume == null) {
                        postMessage("Select a volume", LOG_NORMAL);
                        return;
                    }
                    final Buffer buffer = _currentVolume.getPool().getBufferCopy(_currentVolume, currentPage);
                    if (summary) {
                        postMessage(buffer.toString(), LOG_NORMAL);
                    } else {
                        postMessage(buffer.toStringDetail(findPointer, maxkey, maxvalue, context, allLines), LOG_NORMAL);
                    }
                    currentPage = buffer.getRightSibling();
                }
            }

            @Override
            public String getStatus() {
                return "";
            }

        };
    }

    @Cmd("jquery")
    Task jquery(final @Arg("page|long:-1|Page address for PageNode to look up") long pageAddress,
            final @Arg("volumeHandle|int:-1|Volume handle for PageNode to look up") int volumeHandle,
            final @Arg("ts|long:-1|Start timestamp of TransactionMapItem to look up") long ts,
            final @Arg("_flag|v|Verbose") boolean verbose,
            final @Arg("_flag|V|Show volume handle map") boolean showTreeMap,
            final @Arg("_flag|T|Show tree handle map") boolean showVolumeMap) {
        return new Task() {
            @Override
            public void runTask() throws Exception {
                if (!showVolumeMap && !showTreeMap && pageAddress == -1 && ts == -1) {
                    postMessage("No items requested", LOG_NORMAL);
                    return;
                }
                if (showVolumeMap) {
                    postMessage("Volume Handle Map", LOG_NORMAL);
                    final Map<Integer, Volume> map = _persistit.getJournalManager().queryVolumeMap();
                    for (final Map.Entry<Integer, Volume> entry : map.entrySet()) {
                        postMessage(String.format("%,5d -> %s", entry.getKey(), entry.getValue()), LOG_NORMAL);
                    }
                }
                if (showVolumeMap) {
                    postMessage("Tree Handle Map", LOG_NORMAL);
                    final Map<Integer, TreeDescriptor> map = _persistit.getJournalManager().queryTreeMap();
                    for (final Map.Entry<Integer, TreeDescriptor> entry : map.entrySet()) {
                        postMessage(String.format("%,5d -> %s", entry.getKey(), entry.getValue()), LOG_NORMAL);
                    }
                }
                if (ts != -1) {
                    final TransactionMapItem item = _persistit.getJournalManager().queryTransactionMap(ts);
                    postMessage(String.format("TransactionMapItem for ts=%,d -> %s", ts, item), LOG_NORMAL);
                }
                if (pageAddress != -1) {
                    postMessage("Page Nodes", LOG_NORMAL);
                    if (volumeHandle != -1) {
                        queryPageNode(volumeHandle, pageAddress, verbose);
                    } else {
                        final Map<Integer, Volume> volumeMap = _persistit.getJournalManager().queryVolumeMap();
                        for (final int handle : volumeMap.keySet()) {
                            queryPageNode(handle, pageAddress, verbose);
                        }
                    }
                }
            }

            private void queryPageNode(final int volumeHandle, final long page, final boolean verbose) {
                PageNode pn = _persistit.getJournalManager().queryPageNode(volumeHandle, pageAddress);
                int count = 0;
                while (pn != null && count++ < MAX_PAGE_NODES) {
                    postMessage(String.format("%,5d: %s", count, pn), LOG_NORMAL);
                    if (!verbose) {
                        break;
                    }
                    pn = pn.getPrevious();
                }
            }

            @Override
            public String getStatus() {
                return "";
            }

        };
    }

    @Cmd("dump")
    Task dump(final @Arg("file|string|Name of file to receive output") String file,
            final @Arg("_flag|s|Secure") boolean secure, final @Arg("_flag|o|Overwrite file") boolean ovewrite,
            final @Arg("_flag|v|Verbose") boolean verbose) throws Exception {

        return new Task() {
            @Override
            public void runTask() throws Exception {
                final File target = new File(file);
                if (target.exists() && !ovewrite) {
                    throw new IOException(file + " already exists");
                }

                final ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(target),
                        BUFFER_SIZE));
                final String basePath = "PersistitDump_" + new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
                final long baseTime = System.currentTimeMillis();

                zos.setLevel(ZipEntry.DEFLATED);
                ZipEntry ze = new ZipEntry(JournalManager.generationToFile(basePath, 0).getPath());
                ze.setSize(Integer.MAX_VALUE);
                ze.setTime(baseTime);
                zos.putNextEntry(ze);

                final DataOutputStream stream = new DataOutputStream(zos);

                final ByteBuffer bb = ByteBuffer.allocate(BUFFER_SIZE);
                {
                    JH.putType(bb);
                    JH.putTimestamp(bb, 0);
                    JH.putVersion(bb, JournalManagerMXBean.VERSION);
                    JH.putBlockSize(bb, HUGE_BLOCK_SIZE);
                    JH.putBaseJournalAddress(bb, 0);
                    JH.putCurrentJournalAddress(bb, 0);
                    JH.putJournalCreatedTime(bb, 0);
                    JH.putFileCreatedTime(bb, 0);
                    JH.putPath(bb, basePath);
                    bb.position(JH.getLength(bb));
                }

                final List<BufferPool> pools = new ArrayList<BufferPool>(_persistit.getBufferPoolHashMap().values());
                for (final BufferPool pool : pools) {
                    pool.dump(stream, bb, secure, verbose);
                }

                {
                    CP.putLength(bb, CP.OVERHEAD);
                    CP.putType(bb);
                    CP.putTimestamp(bb, _persistit.getTimestampAllocator().getCurrentTimestamp() + 1);
                    CP.putSystemTimeMillis(bb, baseTime);
                    CP.putBaseAddress(bb, 0);
                    bb.position(CP.OVERHEAD);
                }

                bb.flip();
                stream.write(bb.array(), 0, bb.limit());
                stream.flush();
                zos.closeEntry();
                bb.clear();

                final PrintWriter writer = new PrintWriter(zos);
                ze = new ZipEntry(basePath + ".txt");
                ze.setSize(Integer.MAX_VALUE);
                ze.setTime(baseTime);
                zos.putNextEntry(ze);
                final List<Volume> volumes = _persistit.getVolumes();

                writer.printf("@volumes=%d\n", volumes.size());
                for (final Volume volume : volumes) {
                    writer.printf("%s\n", volume.toString());
                    final List<Tree> trees = volume.getStructure().referencedTrees();
                    writer.printf("@trees=%d\n", trees.size());
                    for (final Tree tree : trees) {
                        writer.printf("%s\n", tree.toString());
                    }
                }
                writer.printf("@bufferPools=%d\n", pools.size());
                for (final BufferPool pool : pools) {
                    writer.printf("%s\n", pool.toString());
                    writer.printf("@buffers=%d\n", pool.getBufferCount());
                    for (int i = 0; i < pool.getBufferCount(); i++) {
                        writer.printf("%s\n", pool.toString(i, false));
                    }
                }
                writer.flush();
                zos.closeEntry();
                stream.close();
            }

            @Override
            public String getStatus() {
                return "";
            }

        };
    }

    @Cmd("help")
    Task help() throws Exception {
        return new Task() {

            @Override
            public void runTask() {
                for (final Command command : COMMANDS.values()) {
                    postMessage(command.toString(), LOG_NORMAL);
                    postMessage("", LOG_NORMAL);
                }
            }

            @Override
            public String getStatus() {
                return "done";
            }

        };
    }

    @Cmd("cliserver")
    static Task cliserver(final @Arg("port|int:9999:1024:99999999") int port) throws Exception {
        final Task task = new Task() {
            CLI _cli;

            @Override
            protected void runTask() throws Exception {
                _cli = new CLI(_persistit, port);
                _cli.commandLoop();
            }

            @Override
            public String getStatus() {
                final CLI cli = _cli;
                if (cli != null) {
                    final String status = cli._lastStatus;
                    if (status != null) {
                        return status;
                    }
                }
                return "Not initialized yet";
            }

        };
        return task;
    }

    private static String journalPath(final List<String> files) {
        String journalPath = null;
        for (final String file : files) {
            final Matcher matcher = JournalManager.PATH_PATTERN.matcher(file);
            if (matcher.matches()) {
                final String path = matcher.group(1);
                if (journalPath == null) {
                    journalPath = path;
                } else if (!journalPath.equals(path)) {
                    throw new IllegalArgumentException("Journal path is not unique: " + journalPath + " / " + path);
                }
            }
        }
        return journalPath;
    }

    private static List<VolumeSpecification> volumeSpecifications(final List<String> files, final long systemTimestamp) {
        final List<VolumeSpecification> list = new ArrayList<VolumeSpecification>();
        for (final String path : files) {
            if (JournalManager.PATH_PATTERN.matcher(path).matches()) {
                continue;
            }
            try {
                final VolumeSpecification specification = new VolumeSpecification(path);
                if (VolumeHeader.verifyVolumeHeader(specification, systemTimestamp)) {
                    list.add(specification);
                }
            } catch (final PersistitException e) {
                // ignore this file
            }
        }
        return list;
    }

    private static List<String> filesOnPath(final String path) {
        final List<String> list = new ArrayList<String>();

        File dir = new File(path);
        String name = "";
        if (!dir.isDirectory()) {
            name = dir.getName();
            dir = dir.getParentFile();
        }
        for (final String candidate : dir.list()) {
            if (candidate.startsWith(name)) {
                list.add(new File(dir, candidate).getPath());
            }
        }
        Collections.sort(list);
        return list;
    }

    static KeyFilter toKeyFilter(final String keyFilterString) {
        if (keyFilterString == null || keyFilterString.isEmpty()) {
            return new KeyFilter();
        } else {
            return new KeyFilter(keyFilterString);
        }
    }

    static Pattern toRegEx(final String pattern, final boolean simple) {
        if (pattern == null || pattern.isEmpty()) {
            return ALL;
        }
        if (simple) {
            final StringBuilder sb = new StringBuilder();
            for (int index = 0; index < pattern.length(); index++) {
                final char c = pattern.charAt(index);
                if (c == '.') {
                    sb.append("\\.");
                } else if (c == '*') {
                    sb.append(".*");
                } else if (c == '?') {
                    sb.append(".");
                } else {
                    sb.append(c);
                }
            }
            return Pattern.compile(sb.toString());
        } else {
            return Pattern.compile(pattern);
        }
    }
}
