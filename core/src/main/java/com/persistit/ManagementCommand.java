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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The parsed specification of a command delivered through the
 * {@link ManagementMXBean#execute(String)} method. This method parses a string
 * representation of a Task specification using {@link ArgParser}.
 * 
 * @author peter
 * 
 */

public class ManagementCommand {

    private final static Map<String, CommandStruct> COMMAND_SPECIFICATIONS = new HashMap<String, CommandStruct>();

    private final static char DEFAULT_COMMAND_DELIMITER = ' ';
    private final static char DEFAULT_QUOTE = '\\';

    private static class CommandStruct {
        private final String _commandName;
        private final Class<?> _class;

        CommandStruct(final String commandName, final Class<?> clazz) {
            _commandName = commandName;
            _class = clazz;
        }
    }

    static {
        COMMAND_SPECIFICATIONS.put(BackupTask.COMMAND_NAME, new CommandStruct(
                BackupTask.COMMAND_NAME, BackupTask.class));

        COMMAND_SPECIFICATIONS.put(IntegrityCheck.COMMAND_NAME,
                new CommandStruct(IntegrityCheck.COMMAND_NAME,
                        IntegrityCheck.class));

        COMMAND_SPECIFICATIONS.put(StreamSaver.COMMAND_NAME, new CommandStruct(
                StreamSaver.COMMAND_NAME, StreamSaver.class));

        COMMAND_SPECIFICATIONS
                .put(StreamLoader.COMMAND_NAME, new CommandStruct(
                        StreamLoader.COMMAND_NAME, StreamLoader.class));

        COMMAND_SPECIFICATIONS.put(TaskCheck.COMMAND_NAME, new CommandStruct(
                TaskCheck.COMMAND_NAME, TaskCheck.class));

        COMMAND_SPECIFICATIONS.put(StatisticsTask.COMMAND_NAME,
                new CommandStruct(StatisticsTask.COMMAND_NAME,
                        StatisticsTask.class));
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
            if (index == 1 && commandDelimiter != DEFAULT_COMMAND_DELIMITER
                    && !Character.isLetter(c) && c != commandDelimiter) {
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

    /**
     * Parse a command line to produce a <code>ManagementCommand</code>.
     * 
     * @param commandLine
     * @return the command
     * @throws IllegalArgumentException
     *             if the command name is unknown
     */
    public static ManagementCommand parse(final String commandLine) {
        final List<String> strings = pieces(commandLine);
        final String commandName = strings.remove(0);
        final String[] args = strings.toArray(new String[strings.size()]);
        final CommandStruct commandStruct = COMMAND_SPECIFICATIONS
                .get(commandName);
        if (commandStruct == null) {
            throw new IllegalArgumentException("No such command: "
                    + commandName);
        }
        return new ManagementCommand(commandStruct, args);
    }

    private final CommandStruct _commandStruct;
    private final String[] _args;

    private ManagementCommand(final CommandStruct commandStruct,
            final String[] args) {
        _commandStruct = commandStruct;
        _args = args;
    }

    public String getCommandName() {
        return _commandStruct._commandName;
    }

    public Class<?> getTaskClass() {
        return _commandStruct._class;
    }

    public String[] getArgs() {
        return _args;
    }
}
