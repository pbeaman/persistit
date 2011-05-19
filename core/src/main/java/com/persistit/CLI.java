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
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;

import com.persistit.exception.PersistitException;

public class CLI {

    public static void main(final String[] args) throws Exception {
        final PrintWriter writer = new PrintWriter(System.out);
        final LineReader reader = lineReader(new BufferedReader(
                new InputStreamReader(System.in)), writer);
        new CLI(reader, writer).run();
    }

    public static long availableMemory() {
        final MemoryUsage mu = ManagementFactory.getMemoryMXBean()
                .getHeapMemoryUsage();
        long max = mu.getMax();
        if (max == -1) {
            max = mu.getInit();
        }
        return max;
    }

    private static String _prompt = "Persistit CLI> ";

    private final LineReader _reader;
    private final PrintWriter _writer;
    private Stack<BufferedReader> _sourceStack = new Stack<BufferedReader>();
    private Persistit _persistit;
    private boolean _stop = false;
    private Volume _currentVolume;
    private Tree _currentTree;
    Map<String, Command> commands = new HashMap<String, Command>();

    interface LineReader {
        public String readLine() throws IOException;
    }

    abstract class Command {
        abstract String[] template();

        abstract String execute(final ArgParser ap) throws Exception;
    }

    private CLI(final LineReader reader, final PrintWriter writer) {
        _reader = reader;
        _writer = writer;

        commands.put("adminui", new CommandAdminUI());
        commands.put("close", new CommandClose());
        commands.put("init", new CommandInit());
        commands.put("journal", new CommandJournal());
        commands.put("path", new CommandPath());
        commands.put("select", new CommandSelect());
        commands.put("source", new CommandSource());
        commands.put("view", new CommandView());
    }

    public void run() throws Exception {
        while (!_stop) {
            final String input;
            if (!_sourceStack.isEmpty()) {
                input = _sourceStack.peek().readLine();
                if (input == null) {
                    _sourceStack.pop();
                    continue;
                }
            } else {
                input = _reader.readLine();
            }
            if (input == null) {
                break;
            }
            final List<String> list = ManagementCommand.pieces(input);
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
                break;
            }

            // Handle intrinsic commands
            final Command command = commands.get(commandName);
            if (command != null) {
                list.remove(0);
                try {
                    final String[] args = list.toArray(new String[list.size()]);
                    final ArgParser ap = new ArgParser(commandName, args,
                            command.template());
                    if (!ap.isUsageOnly()) {
                        String result = command.execute(ap);
                        if (result != null) {
                            _writer.println(result);
                            _writer.flush();
                        }
                    }
                } catch (Exception e) {
                    _writer.println(e);
                    _writer.flush();
                }
                continue;
            }

            // Handle ManagementCommands
            if (_persistit != null) {
                try {
                    final String result = _persistit.getManagement().execute(
                            input);
                    if (result != null) {
                        _writer.println(result);
                    }
                } catch (IllegalArgumentException e) {
                    _writer.println(e);
                }
                continue;
            }
            _writer.println("No such command " + commandName);
        }
    }

    class CommandInit extends Command {
        String[] template() {
            return new String[] { "datapath|string|Data path",
                    "journalpath|string|Journal path",
                    "volumepath|string|Volume file",
                    "rmiport|int:1099:0:99999|RMI Management port",
                    "_flag|g|Show AdminUI",
                    "_flag|y|Recover committed transactions" };
        }

        String execute(final ArgParser ap) throws Exception {
            return init(ap);
        }
    }

    class CommandJournal extends Command {

        String[] template() {
            return JournalTool.ARGS_TEMPLATE;
        }

        String execute(final ArgParser ap) throws Exception {
            final JournalTool jt = new JournalTool(_persistit);
            jt.init(ap);
            jt.setWriter(_writer);
            jt.scan();
            return null;
        }
    }

    class CommandSource extends Command {
        String[] template() {
            return new String[] { "file|string|Read commands from file" };
        }

        String execute(final ArgParser ap) throws Exception {
            final String fileName = ap.getStringValue("file");
            if (!fileName.isEmpty()) {
                final FileReader in = new FileReader(fileName);
                _sourceStack.push(new BufferedReader(new BufferedReader(in)));
                return String.format("Source is %s", fileName);
            } else {
                _sourceStack.clear();
                return "Source is console";
            }
        }
    }

    class CommandClose extends Command {
        String[] template() {
            return new String[] {};
        }

        String execute(final ArgParser ap) throws Exception {
            return close(ap.isFlag('f'));
        }
    }

    class CommandAdminUI extends Command {
        String[] template() {
            return new String[] { "_flag|x|Stop", "_flag|g|Start" };
        }

        String execute(final ArgParser ap) throws Exception {

            if (ap.isFlag('g')) {
                _persistit.setupGUI(false);
                return "Started AdminUI";
            }
            if (ap.isFlag('x')) {
                _persistit.shutdownGUI();
                return "Stopped AdminUI";
            }
            return null;
        }
    }

    class CommandSelect extends Command {
        String[] template() {
            return new String[] { "volume|string|Volume name",
                    "tree|string|Tree name" };
        }

        String execute(final ArgParser ap) throws Exception {
            if (_persistit == null) {
                return "Not initialized";
            }
            String vname = ap.getStringValue("volume");

            if (!vname.isEmpty()) {
                Volume volume = _persistit.getVolume(vname);
                if (volume == null) {
                    return String.format("No volume named %s", vname);
                }
                _currentVolume = volume;
            }

            String tname = ap.getStringValue("tree");
            if (!tname.isEmpty()) {
                Tree tree = _currentVolume.getTree(tname, false);
                if (tree == null) {
                    return String.format("No tree in %s named %s",
                            _currentVolume, tname);
                } else {
                    _currentTree = tree;
                }
            }
            return String.format("Volume %s tree %s", _currentVolume,
                    _currentTree);
        }
    }

    class CommandPath extends Command {
        String[] template() {
            return new String[] { "key|string|Key" };
        }

        String execute(final ArgParser ap) throws Exception {
            if (_persistit == null) {
                return "Not initialized";
            }
            if (_currentVolume == null || _currentTree == null) {
                return "Tree not selected";
            }
            final Exchange exchange = new Exchange(_currentTree);
            final String keyString = ap.getStringValue("key");
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
    }

    class CommandView extends Command {
        String[] template() {
            return new String[] { "page|long:0:0:999999999999|Page address",
                    "level|int:0:0:20|Tree level", "key|string|Key",
                    "_flag|a|All lines", "_flag|s|Summary only" };
        }

        String execute(final ArgParser ap) throws Exception {
            if (_persistit == null) {
                return "Not initialized";
            }
            final Buffer buffer;
            long pageAddress = ap.getLongValue("page");
            if (pageAddress > 0) {
                if (_currentVolume == null) {
                    return "Select a volume";
                }
                buffer = _currentVolume.getPool().getBufferCopy(_currentVolume,
                        pageAddress);
            } else {
                if (_currentTree == null) {
                    return "Select a tree";
                }
                final Exchange exchange = new Exchange(_currentTree);
                final int level = ap.getIntValue("level");
                final String keyString = ap.getStringValue("key");
                if (!keyString.isEmpty()) {
                    new KeyParser(keyString).parseKey(exchange.getKey());
                }
                buffer = exchange.fetchBufferCopy(level);
            }
            if (ap.isFlag('s')) {
                return buffer.toString();
            }
            String detail = buffer.toStringDetail();
            if (ap.isFlag('a')) {
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
    }

    private String close(final boolean flush) throws Exception {
        if (_persistit != null) {
            try {
                _persistit.shutdownGUI();
                _persistit.close(flush);
            } catch (Exception e) {
                return e.toString();
            } finally {
                _persistit = null;
            }
        }
        return "ok";
    }

    private String init(final ArgParser ap) throws Exception {

        close(false);

        String datapath = ap.getStringValue("datapath");
        String journalpath = ap.getStringValue("journalpath");
        String volumepath = ap.getStringValue("volumepath");
        int rmiport = ap.getIntValue("rmiport");
        boolean recover = ap.isFlag('y');

        String jpath = journalPath(filesOnPath(journalpath.isEmpty() ? datapath
                : journalpath));
        List<VolumeSpecification> volumeSpecifications = volumeSpecifications(filesOnPath(volumepath
                .isEmpty() ? datapath : volumepath));
        Set<Integer> bufferSizes = new HashSet<Integer>();
        for (final VolumeSpecification vs : volumeSpecifications) {
            bufferSizes.add(vs.getBufferSize());
        }
        final Properties properties = new Properties();
        long bpoolMemory = availableMemory() / 2;
        for (final Integer size : bufferSizes) {
            int alloc = (int) (size * 1.25);
            final int count = (int) ((bpoolMemory / bufferSizes.size()) / alloc);
            properties.put(Persistit.BUFFERS_PROPERTY_NAME + size,
                    Integer.toString(count));
        }
        int index = 0;
        for (final VolumeSpecification vs : volumeSpecifications) {
            String value = vs.toString();
            if (!recover) {
                value += ",readOnly";
            }
            properties.put(Persistit.VOLUME_PROPERTY_PREFIX + (++index), value);
        }
        properties.put(Persistit.JOURNAL_PATH_PROPERTY_NAME, jpath);
        properties.put(Persistit.APPEND_ONLY_PROPERTY, "true");

        if (rmiport > 0) {
            properties.put(Persistit.RMI_REGISTRY_PORT,
                    Integer.toString(rmiport));
        }
        if (ap.isFlag('g')) {
            properties.put(Persistit.SHOW_GUI_PROPERTY, "true");
        }
        properties.put(Persistit.JMX_PARAMS, "true");

        final Persistit persistit = new Persistit();
        if (!recover) {
            persistit.getRecoveryManager().setRecoveryDisabledForTestMode(true);
        }
        persistit.initialize(properties);
        _persistit = persistit;
        return "ok";
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
                    throw new IllegalArgumentException(
                            "Journal path is not unique: " + journalPath
                                    + " / " + path);
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
                    list.add(new VolumeSpecification(path + ",pageSize:"
                            + volumeInfo._bufferSize + ",id:" + volumeInfo._id));
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

    // TODO - we need to refactor Volume!
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

    /**
     * Uses reflection to try to find a jline.ConsoleReader. The jline package
     * supports command history and in-line editing. If the jline is not in the
     * classpath, then this method returns a <code>LineReader</code> based on
     * {@link System#in}.
     * 
     * @return A <code>LineReader</code>
     * @see http://jline.sourceforge.net/
     */
    private static LineReader lineReader(final Reader reader,
            final PrintWriter writer) {
        LineReader lineReader = null;
        ;
        try {
            final Class<?> readerClass = CLI.class.getClassLoader().loadClass(
                    "jline.ConsoleReader");
            final Object consoleReader = readerClass.newInstance();
            final Method readLineMethod = readerClass.getMethod("readLine",
                    new Class[] { String.class });
            lineReader = new LineReader() {
                public String readLine() throws IOException {
                    try {
                        return (String) readLineMethod.invoke(consoleReader,
                                new Object[] { _prompt });
                    } catch (IllegalArgumentException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            writer.println("jline.ConsoleReader enabled");
        } catch (Exception e) {

        }
        if (lineReader == null) {
            final BufferedReader br = new BufferedReader(reader);
            lineReader = new LineReader() {
                public String readLine() throws IOException {
                    writer.print(_prompt);
                    writer.flush();
                    return br.readLine();
                }
            };
        }
        return lineReader;
    }
}
