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
        private final String[] _argTemplate;

        CommandStruct(final String commandName, final Class<?> clazz,
                final String[] argTemplate) {
            _commandName = commandName;
            _class = clazz;
            _argTemplate = argTemplate;
        }
    }

    static {
        COMMAND_SPECIFICATIONS.put(Backup.COMMAND_NAME, new CommandStruct(
                Backup.COMMAND_NAME, Backup.class,
                Backup.ARG_TEMPLATE));
        
        COMMAND_SPECIFICATIONS.put(IntegrityCheck.COMMAND_NAME,
                new CommandStruct(IntegrityCheck.COMMAND_NAME,
                        IntegrityCheck.class, IntegrityCheck.ARG_TEMPLATE));
        
        COMMAND_SPECIFICATIONS.put(StreamSaver.COMMAND_NAME, new CommandStruct(
                StreamSaver.COMMAND_NAME, StreamSaver.class,
                StreamSaver.ARG_TEMPLATE));
        
        COMMAND_SPECIFICATIONS.put(StreamLoader.COMMAND_NAME,
                new CommandStruct(StreamLoader.COMMAND_NAME,
                        StreamLoader.class, StreamLoader.ARG_TEMPLATE));
        
        COMMAND_SPECIFICATIONS.put(TaskCheck.COMMAND_NAME,
                new CommandStruct(TaskCheck.COMMAND_NAME, TaskCheck.class,
                        TaskCheck.ARG_TEMPLATE));
    }

    public static ManagementCommand parse(final String commandLine) {
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
        if (strings.isEmpty()) {
            throw new IllegalArgumentException("No command name specified");
        }
        final String commandName = strings.remove(0);
        final String[] args = strings.toArray(new String[strings.size()]);
        final CommandStruct commandStruct = COMMAND_SPECIFICATIONS
                .get(commandName);
        if (commandStruct == null) {
            throw new IllegalArgumentException("No such command: "
                    + commandName);
        }
        return new ManagementCommand(commandStruct, new ArgParser(commandName,
                args, commandStruct._argTemplate));
    }

    private final CommandStruct _commandStruct;
    private final ArgParser _argParser;

    private ManagementCommand(final CommandStruct commandStruct,
            final ArgParser argParser) {
        _commandStruct = commandStruct;
        _argParser = argParser;
    }

    public String getCommandName() {
        return _commandStruct._commandName;
    }

    public Class<?> getTaskClass() {
        return _commandStruct._class;
    }

    public ArgParser getArgParser() {
        return _argParser;
    }
}
