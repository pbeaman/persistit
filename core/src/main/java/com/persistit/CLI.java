/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.persistit.exception.PersistitException;
import com.persistit.util.ArgParser;
import com.persistit.util.Util;

/**
 * Handle commands delivered interactively as command strings.  For example,
 * the following loads a Persistit database located in directory /var/lib/data,
 * selects a volume named xyz, and displays page 42 of that volume:
 * <code><pre>
 * load datapath=/var/lib/data
 * select volume=xyz
 * view page=42
 * </pre></code>
 * <p />
 * Command lines can be entered interactively from stdin and stdout. Alternatively,
 * CLI can set up an extremely simple network server that works with telnet or curl.
 * The advantage is that you can then issue commands interactively
 * from a simple network client using the full facilities of the shell, including
 * piping the output to tools such as grep and more.
 * <p />
 * To run the CLI, simple execute
 * <code><pre>
 * java -cp persisit.jar com.persistit.CLI
 * </pre></code>
 * where persistit.jar contains the Persistit library. If you want to use the network
 * version, add the argument port=NNNN to specify a port on which CLI will listen for
 * commands. (Use a port number larger than 1024 to avoid permissions problems.)
 * <p />
 * Commands are defined below in methods annotated with @Cmd having parameters annotated with @Arg.
 * The format of the argument annotation is specified in {@link ArgParser). Enter the command
 * 'help' to see a readable list of commands and their arguments.
 * <p />
 * The following client script works with the network server facility:
 * <code><pre>
 *   #!/bin/sh
 *   echo "$*" | curl telnet://localhost:9999
 * </pre></code>
 * (The echo pipeline trick does not seem to work with telnet but does work with curl.) 
 * You can then enter command such as this at the shell:
 * 
 * <code><pre>
 * pcli init datapath=/var/lib/data
 * pcli select volume=xyz
 * pcli view page=42
 * </code></pre>
 * 
 * Note that the network server closes the client socket on each request, so a new connection
 * is formed each time you invoke curl or telnet.
 * 
 * 
 * 
 * @author peter
 *
 */
public class CLI {
    private final static char DEFAULT_COMMAND_DELIMITER = ' ';
    private final static char DEFAULT_QUOTE = '\\';
    private final static Map<String, Command> COMMANDS = new TreeMap<String, Command>();

    private final static Class<?>[] CLASSES = { CLI.class, BackupTask.class, IntegrityCheck.class, StreamSaver.class,
            StreamLoader.class, StatisticsTask.class, TaskCheck.class };

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
                String name = method.getAnnotation(Cmd.class).value();
                Annotation[][] parameters = method.getParameterAnnotations();
                String[] argTemplate = new String[parameters.length];
                int index = 0;
                for (Annotation[] annotations : parameters) {
                    Arg argAnnotation = (Arg) annotations[0];
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

        String[] hostPieces = args[0].split(":");
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
            char c = commandLine.charAt(index);
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

    public static Task parseTask(final Persistit persistit, final String line) throws Exception {
        List<String> pieces = pieces(line);
        if (pieces.isEmpty()) {
            return null;
        }
        final String commandName = pieces.remove(0);
        Command command = COMMANDS.get(commandName);
        if (command == null) {
            return null;
        }
        Task task = command.createTask(new ArgParser(commandName, pieces.toArray(new String[pieces.size()]),
                command.argTemplate));
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

    private static class Command {
        final String name;
        final String[] argTemplate;
        final Method method;

        private Command(final String name, final String[] argTemplate, final Method method) {
            this.name = name;
            this.argTemplate = argTemplate;
            this.method = method;
        }

        private String execute(final CLI cli, final ArgParser ap, final boolean immediate) throws Exception {
            final Object[] args = invocationArgs(ap);
            if (method.getReturnType() == String.class) {
                String result = (String) method.invoke(cli, args);
                return result;
            } else if (Task.class.isAssignableFrom(method.getReturnType())) {
                Task task = (Task) method.invoke(cli, args);
                task.setPersistit(cli._persistit);
                task.setMaximumTime(-1);
                task.setMessageWriter(cli._writer);
                if (immediate || task.isImmediate()) {
                    task.runTask();
                } else {
                    return cli._persistit.getManagement().launch(task, name);
                }
                return task.getStatus();
            } else {
                throw new IllegalStateException(this + " must return either a Task or a String");
            }
        }

        private Task createTask(final ArgParser ap) throws Exception {
            if (Task.class.isAssignableFrom(method.getReturnType())) {
                final Object[] args = invocationArgs(ap);
                Task task = (Task) method.invoke(null, args);
                return task;
            } else {
                return null;
            }
        }

        private Object[] invocationArgs(final ArgParser ap) {
            Class<?>[] types = method.getParameterTypes();
            final Object[] args = new Object[types.length];
            for (int index = 0; index < types.length; index++) {
                Class<?> type = types[index];
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
            StringBuilder sb = new StringBuilder(name);
            sb.append(Util.NEW_LINE);
            sb.append(new ArgParser(name, new String[0], argTemplate));
            return sb.toString();
        }
    }

    private static class NotOpenException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    // ----------------------

    private LineReader _lineReader;
    PrintWriter _writer = new PrintWriter(System.out);
    private Stack<BufferedReader> _sourceStack = new Stack<BufferedReader>();
    private Persistit _persistit;
    private boolean _stop = false;
    private Volume _currentVolume;
    private Tree _currentTree;
    private final boolean _live;
    private int _commandCount;
    private String _lastStatus;

    public CLI(final Persistit persistit, final int port) throws IOException {

        _lineReader = new NetworkReader(port);
        _persistit = persistit;
        _live = persistit != null;
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
                String commandName = list.remove(0);
                if (commandName.startsWith("#")) {
                    continue;
                }

                if (EXIT_COMMAND.equals(commandName)) {
                    _stop = true;
                    close(false);
                    _lastStatus = "Done";
                    _writer.println("Done");
                    _lineReader.close();
                }

                boolean immediate = true;
                if (commandName.equals(LAUNCH_COMMAND)) {
                    immediate = false;
                    commandName = list.remove(0);
                }

                // Handle intrinsic commands
                final Command command = COMMANDS.get(commandName);
                if (command != null) {
                    try {
                        final String[] args = list.toArray(new String[list.size()]);
                        final ArgParser ap = new ArgParser(commandName, args, command.argTemplate);
                        if (!ap.isUsageOnly()) {
                            String result = command.execute(this, ap, immediate);
                            if (result != null) {
                                _writer.println(result);
                            }
                            _lastStatus += " - done";
                        }
                    } catch (InvocationTargetException e) {
                        _lastStatus += e.getTargetException();
                        _writer.println(e.getTargetException());
                    } catch (RuntimeException e) {
                        _lastStatus += e;
                        e.printStackTrace(_writer);
                    } catch (Exception e) {
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

    @Cmd("open")
    String open(@Arg("datapath|string|Data path") String datapath,
            @Arg("journalpath|string|Journal path") String journalpath,
            @Arg("volumepath|string|Volume file") String volumepath,
            @Arg("rmiport|int:1099:0:99999|RMI Management port") int rmiport, @Arg("_flag|g|Show AdminUI") boolean g,
            @Arg("_flag|y|Recover committed transactions") boolean y) throws Exception {
        close(false);

        String jpath = journalPath(filesOnPath(journalpath.isEmpty() ? datapath : journalpath));
        List<VolumeSpecification> volumeSpecifications = volumeSpecifications(filesOnPath(volumepath.isEmpty() ? datapath
                : volumepath));
        Set<Integer> bufferSizes = new HashSet<Integer>();
        for (final VolumeSpecification vs : volumeSpecifications) {
            bufferSizes.add(vs.getBufferSize());
        }
        final Properties properties = new Properties();
        long bpoolMemory = availableMemory() / 2;
        for (final Integer size : bufferSizes) {
            int alloc = (int) (size * 1.25);
            final int count = (int) ((bpoolMemory / bufferSizes.size()) / alloc);
            properties.put(Persistit.BUFFERS_PROPERTY_NAME + size, Integer.toString(count));
        }
        int index = 0;
        for (final VolumeSpecification vs : volumeSpecifications) {
            String value = vs.toString();
            if (!y) {
                value += ",readOnly";
            }
            properties.put(Persistit.VOLUME_PROPERTY_PREFIX + (++index), value);
        }
        if (jpath != null) {
            properties.put(Persistit.JOURNAL_PATH_PROPERTY_NAME, jpath);
        }
        properties.put(Persistit.APPEND_ONLY_PROPERTY, "true");

        if (rmiport > 0) {
            properties.put(Persistit.RMI_REGISTRY_PORT, Integer.toString(rmiport));
        }
        if (g) {
            properties.put(Persistit.SHOW_GUI_PROPERTY, "true");
        }
        properties.put(Persistit.JMX_PARAMS, "true");

        final Persistit persistit = new Persistit();
        if (!y) {
            persistit.getRecoveryManager().setRecoveryDisabledForTestMode(true);
        }
        persistit.initialize(properties);
        
        /**
         * Following is a hack to figure ought whether there is a classIndex in
         * exactly one volume, and if so, make is the system volume. There should
         * be an API in the Persistit class itself to do this, but currently there
         * isn't one.
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
            properties.put(Persistit.SYSTEM_VOLUME_PROPERTY, sysvol.getName());
        }
        
        
        _persistit = persistit;
        return "Last valid checkpoint=" + persistit.getRecoveryManager().getLastValidCheckpoint().toString();
    }

    @Cmd("list")
    String list(@Arg("volume|string|Volume name") String vstring, @Arg("tree|string|Tree name") String tstring,
            @Arg("_flag|r|Regular expression") boolean r) throws Exception {
        if (_persistit == null) {
            return "Persistit not loaded";
        }
        final Pattern vpattern = toRegEx(vstring, !r);
        final Pattern tpattern = toRegEx(vstring, !r);
        final StringBuilder sb = new StringBuilder();
        for (final Volume volume : _persistit.getVolumes()) {
            if (vpattern.matcher(volume.getName()).matches()) {
                sb.append(volume);
                sb.append(Util.NEW_LINE);
                for (final String treeName : volume.getTreeNames()) {
                    if (tpattern.matcher(treeName).matches()) {
                        final Tree tree = volume.getTree(treeName, false);
                        sb.append("   ");
                        sb.append(tree);
                        sb.append(Util.NEW_LINE);
                    }
                }
            }
        }
        return sb.toString();
    }

    @Cmd("journal")
    String journal(@Arg("path|string:|Journal file name") String path,
            @Arg("start|long:0:0:10000000000000|Start journal address") long start,
            @Arg("end|long:1000000000000000000:0:1000000000000000000|End journal address") long end,
            @Arg("types|String:*|Selected record types, for example, \"PA,PM,CP\"") String types,
            @Arg("pages|String:*|Selected pages, for example, \"0,1,200-299,33333-\"") String pages,
            @Arg("timestamps|String:*|Selected timestamps, for example, \"132466-132499\"") String timestamps,
            @Arg("maxkey|int:42:4:10000|Maximum displayed key length") int maxkey,
            @Arg("maxvalue|int:42:4:100000|Maximum displayed value length") int maxvalue,
            @Arg("_flag|v|Verbose dump - includes PageMap and TransactionMap details") boolean v) throws Exception {
        final JournalTool jt = new JournalTool(_persistit);
        jt.init(path, start, end, types, pages, timestamps, maxkey, maxvalue, v);
        jt.setWriter(_writer);
        jt.scan();
        return null;
    }

    @Cmd("source")
    String source(@Arg("file|string|Read commands from file") String fileName) throws Exception {
        if (!fileName.isEmpty()) {
            final FileReader in = new FileReader(fileName);
            _sourceStack.push(new BufferedReader(new BufferedReader(in)));
            return String.format("Source is %s", fileName);
        } else {
            _sourceStack.clear();
            return "Source is console";
        }
    }

    @Cmd("close")
    String close(@Arg("_flag|f|Flush modifications to disk") boolean flush) throws Exception {
        if (_persistit != null) {
            try {
                if (_live) {
                    return "Detaching from live Persistit instance without closing it";
                } else {
                    _persistit.shutdownGUI();
                    _persistit.close(flush);
                }
            } catch (Exception e) {
                return e.toString();
            } finally {
                _persistit = null;
                _currentVolume = null;
                _currentTree = null;
            }
        }
        return "ok";
    }

    @Cmd("adminui")
    String adminui(@Arg("_flag|g|Start") boolean g, @Arg("_flag|x|Stop") boolean x) throws Exception {
        if (_persistit == null) {
            return "Persistit not loaded";
        }
        if (g) {
            _persistit.setupGUI(false);
            return "Started AdminUI";
        }
        if (x) {
            _persistit.shutdownGUI();
            return "Stopped AdminUI";
        }
        return "No action specified";
    }

    @Cmd("select")
    String select(@Arg("volume|string|Volume name") String vstring, @Arg("tree|string|Tree name") String tstring,
            @Arg("_flag|r|Regular expression") boolean r) throws Exception {
        if (_persistit == null) {
            return "Persistit not loaded";
        }

        final StringBuilder sb = new StringBuilder();

        Volume selectedVolume = null;
        Tree selectedTree = null;

        boolean tooMany = false;

        final Pattern vpattern = toRegEx(vstring, !r);
        final Pattern tpattern = toRegEx(tstring, !r);
        for (final Volume volume : _persistit.getVolumes()) {
            if (vpattern.matcher(volume.getName()).matches()) {
                if (selectedVolume == null) {
                    selectedVolume = volume;
                } else if (!tooMany) {
                    tooMany = true;
                    sb.append("Multiple volumes - select one");
                    sb.append(Util.NEW_LINE);
                    sb.append(selectedVolume);
                    sb.append(Util.NEW_LINE);
                }
                sb.append(volume);
                sb.append(Util.NEW_LINE);
            }
        }
        if (tooMany) {
            return sb.toString();
        }
        if (selectedVolume != null) {
            _currentVolume = selectedVolume;
        }
        if (_currentVolume == null) {
            return "No volume selected";
        }
        sb.setLength(0);
        for (final String treeName : _currentVolume.getTreeNames()) {
            if (tpattern.matcher(treeName).matches()) {
                final Tree tree = _currentVolume.getTree(treeName, false);
                if (selectedTree == null) {
                    selectedTree = tree;
                } else if (!tooMany) {
                    tooMany = true;
                    sb.append("Multiple trees - select one");
                    sb.append(Util.NEW_LINE);
                    sb.append(selectedTree);
                    sb.append(Util.NEW_LINE);
                }
                sb.append(tree);
                sb.append(Util.NEW_LINE);
            }
        }
        if (tooMany) {
            return sb.toString();
        }
        if (selectedTree != null) {
            _currentTree = selectedTree;
        }
        if (_currentTree == null) {
            return String.format("Volume %s selected", _currentVolume);
        } else {
            return String.format("Volume %s tree %s selected", _currentVolume, _currentTree);
        }
    }

    @Cmd("path")
    String path(@Arg("key|string|Key") String keyString) throws Exception {

        if (_persistit == null) {
            return "Persistit not loaded";
        }

        if (_currentVolume == null || _currentTree == null) {
            return "Tree not selected";
        }
        final Exchange exchange = new Exchange(_currentTree);
        if (!keyString.isEmpty()) {
            new KeyParser(keyString).parseKey(exchange.getKey());
        }
        StringBuilder sb = new StringBuilder();
        int depth = _currentTree.getDepth();
        for (int level = depth; --level >= 0;) {
            Buffer copy = exchange.fetchBufferCopy(level);
            if (sb.length() > 0) {
                sb.append(Util.NEW_LINE);
            }
            sb.append(copy);
        }
        return sb.toString();
    }

    @Cmd("view")
    String view(@Arg("page|long:-1:-1:99999999999999999|Page address") long pageAddress,
            @Arg("jaddr|long:-1:-1:99999999999999999|Journal address of a PA page record") long journalAddress,
            @Arg("level|int:0:0:30|Tree level") int level, @Arg("key|string|Key") String keyString,
            @Arg("find|long:-1:0:99999999999999999|Optional page pointer to find") long findPointer,
            @Arg("_flag|a|All lines") boolean allLines, @Arg("_flag|s|Summary only") boolean summary) throws Exception {

        if (_persistit == null) {
            return "Persistit not loaded";
        }

        final Buffer buffer;
        int specified = 0;
        if (pageAddress >= 0) {
            specified++;
        }
        if (journalAddress >= 0) {
            specified++;
        }
        if (!keyString.isEmpty()) {
            specified++;
        }
        if (specified != 1) {
            return "Specify one of key=<key>, page=<page address> or journal=<journal address>";
        }
        if (journalAddress >= 0) {
            buffer = _persistit.getJournalManager().readPageBuffer(journalAddress);
            if (buffer == null) {
                return String.format("Journal address %,d is not a valid PA record", journalAddress);
            }
            buffer.setValid();
        } else if (pageAddress >= 0) {
            if (_currentVolume == null) {
                return "Select a volume";
            }
            buffer = _currentVolume.getPool().getBufferCopy(_currentVolume, pageAddress);
        } else {
            if (_currentTree == null) {
                return "Select a tree";
            }
            final Exchange exchange = new Exchange(_currentTree);

            if (!keyString.isEmpty()) {
                new KeyParser(keyString).parseKey(exchange.getKey());
            }
            buffer = exchange.fetchBufferCopy(level);
        }
        if (summary) {
            return buffer.toString();
        }
        String detail = buffer.toStringDetail(findPointer);
        if (allLines) {
            return detail;
        } else {
            int p = -1;
            for (int i = 0; i < 20; i++) {
                p = detail.indexOf('\n', p + 1);
                if (p == -1) {
                    return detail;
                }
            }
            return detail.substring(0, p);
        }

    }

    @Cmd("help")
    String help() throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (final Command command : COMMANDS.values()) {
            sb.append(Util.NEW_LINE);
            sb.append(command.toString());
        }
        return sb.toString();
    }

    @Cmd("cliserver")
    static Task cliserver(@Arg("port|int:9999:1024:99999999") final int port) throws Exception {
        Task task = new Task() {
            CLI _cli;

            @Override
            protected void runTask() throws Exception {
                _cli = new CLI(_persistit, port);
                _cli.commandLoop();
            }

            @Override
            public String getStatus() {
                CLI cli = _cli;
                if (cli != null) {
                    String status = cli._lastStatus;
                    if (status != null) {
                        return status;
                    }
                }
                return "Not initialized yet";
            }

        };
        return task;
    }

    private String journalPath(List<String> files) {
        String journalPath = null;
        for (final String file : files) {
            Matcher matcher = JournalManager.PATH_PATTERN.matcher(file);
            if (matcher.matches()) {
                String path = matcher.group(1);
                if (journalPath == null) {
                    journalPath = path;
                } else if (!journalPath.equals(path)) {
                    throw new IllegalArgumentException("Journal path is not unique: " + journalPath + " / " + path);
                }
            }
        }
        return journalPath;
    }

    private List<VolumeSpecification> volumeSpecifications(List<String> files) {
        final List<VolumeSpecification> list = new ArrayList<VolumeSpecification>();
        for (final String path : files) {
            if (JournalManager.PATH_PATTERN.matcher(path).matches()) {
                continue;
            }
            try {
                final File file = new File(path);
                final VolumeInfo volumeInfo = volumeInfo(file);
                if (volumeInfo != null) {
                    list.add(new VolumeSpecification(path + ",pageSize:" + volumeInfo._bufferSize + ",id:"
                            + volumeInfo._id));
                }
            } catch (IOException e) {
                // ignore this file
            } catch (PersistitException e) {
                // ignore this file
            }
        }
        return list;
    }

    private List<String> filesOnPath(final String path) {
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

    private VolumeInfo volumeInfo(final File candidate) throws IOException {
        if (!candidate.exists() || !candidate.isFile()) {
            return null;
        }
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(candidate, "r");
            final VolumeHeader header = new VolumeHeader(raf.getChannel());
            final byte[] bytes = header.validate().array();
            return new VolumeInfo(bytes);
        } catch (PersistitException pe) {
            return null;
        } finally {
            if (raf != null) {
                raf.close();
            }
        }
    }

    // TODO - we REALLY need to refactor Volume!
    //
    static class VolumeInfo {
        int _bufferSize;
        long _id;
        long _readCounter;
        long _writeCounter;
        long _highestPageUsed;
        long _pageCount;
        long _extensionPages;
        long _maximumPages;
        long _firstAvailablePage;
        long _directoryRootPage;
        long _garbageRoot;
        long _initialPages;

        VolumeInfo(final byte[] bytes) {
            _bufferSize = Util.getInt(bytes, 20);
            _id = Util.getLong(bytes, 32);
            _readCounter = Util.getLong(bytes, 40);
            _writeCounter = Util.getLong(bytes, 48);
            _highestPageUsed = Util.getLong(bytes, 104);
            _pageCount = Util.getLong(bytes, 112);
            _extensionPages = Util.getLong(bytes, 120);
            _maximumPages = Util.getLong(bytes, 128);
            _firstAvailablePage = Util.getLong(bytes, 136);
            _directoryRootPage = Util.getLong(bytes, 144);
            _garbageRoot = Util.getLong(bytes, 152);
            _initialPages = Util.getLong(bytes, 192);
        }
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
