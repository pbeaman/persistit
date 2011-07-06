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

package com.persistit.util;

/**
 * <p>
 * A simple command line argument parser the provides primitive type conversion
 * and value checking. An application passes a template and the command line
 * argument array to the constructor of this class. The constructor validates
 * the argument list. Subsequently the application can access specific fields
 * from the command line using the <code>ArgParser</code>'s accessor methods.
 * </p>
 * <p>
 * If the command line includes the argument <code>-?</code> then
 * <code>ArgParser</code> displays a summary of permissible arguments and sets
 * its UsageOnly property to true. The calling application should simply exit if
 * <code>isUsageonly</code> is true for the constructed ArgParser. The display
 * text output is in English. Applications needing localization should use a
 * different mechanism.
 * </p>
 * 
 * @author peter
 * @version 1.0
 */
public class ArgParser {
    private final String _progName;
    private final String[] _template;
    private final String _flags;
    private final String[] _strArgs;
    private final long[] _longArgs;
    private final boolean[] _isDefault;
    private boolean _usageOnly;

    /**
     * <p>
     * Construct an instance that parses an array of command line arguments
     * according to specifications provided in a template. The supplied template
     * is an array of specification elements.
     * </p>
     * <p>
     * Command line arguments are specified by name, not by position. Each
     * argument must either be a flag in the form <code>-<i>X</i></code> (where
     * <i>X</i> is letter) or a name/value pair in the form
     * <code><i>argname</i>=</i>value</i></code>. The permissible flags and
     * argument names are specified by the array of template strings, each of
     * which must have the form:
     * </p>
     * </p> <blockquote>
     * 
     * <pre>
     * <code>  _flag|<i>flchar</i>|<i>description</i></code>
     *         or
     * <code>  <i>argname</i>|<i>argtype</i>|<i>description</i></code>
     * </pre>
     * 
     * </blockquote> </p>
     * <p>
     * where
     * </p>
     * <dl>
     * <dt><code><i>flchar</i></code></dt>
     * <dd>is a single letter that can be used as a flag on the command line.
     * For example, to allow a the flag "-x", use a template string of the form
     * <code>_flags|x|Enable the x option</code>.</dd>
     * <dt><code><i>argname</i></code></dt>
     * <dd>Parameter name.</dd>
     * </dd>
     * <dt><code><i>argtype</i></code></dt>
     * <dd>
     * 
     * <pre>
     * <code>int:<i>defaultvalue</i>:<i>lowbound</i>:<i>highbound</i></code>
     *        or
     * <code>String:<i>default</i></code>
     * </pre>
     * 
     * </dd>
     * </dl>
     * </p>
     */
    public ArgParser(String progName, String[] args, String[] template) {
        _progName = progName;
        _template = template;
        _strArgs = new String[template.length];
        _longArgs = new long[template.length];
        _isDefault = new boolean[template.length];

        StringBuilder flags = new StringBuilder();

        String flagsTemplate = "";
        for (int i = 0; i < template.length; i++) {
            if (template[i].startsWith("_flag|")) {
                flagsTemplate += piece(template[i], '|', 1);
            } else {
                doField(null, i);
            }
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                for (int j = 1; j < arg.length(); j++) {
                    final char ch = arg.charAt(j);
                    if (ch == '?') {
                        usage();
                    } else if (flagsTemplate.indexOf(ch) >= 0) {
                        flags.append(ch);
                    } else {
                        throw new IllegalArgumentException("Invalid flag (" + ch + ") in " + arg);
                    }
                }
            } else {
                String fieldName = piece(args[i], '=', 0);
                int position = lookupName(fieldName);
                if (position < 0)
                    throw new IllegalArgumentException("No such parameter name " + fieldName + " in argument " + arg);
                String argValue = arg.substring(fieldName.length() + 1);
                doField(argValue, position);
            }
        }
        _flags = flags.toString();

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < _template.length; i++) {
            String t = _template[i];
            if (t.startsWith("_flag|")) {
                sb.append("  flag -");
                sb.append(piece(t, '|', 1));
                tab(sb, 24);
                sb.append(piece(t, '|', 2));
            } else {
                sb.append("  ");
                sb.append(piece(t, '|', 0));
                tab(sb, 24);
                sb.append(piece(t, '|', 2));
            }
            sb.append(Util.NEW_LINE);
        }
        return sb.toString();
    }

    /**
     * Display a description of the permissible argument values to
     * {@link java.lang.System#out}.
     */
    public void usage() {
        _usageOnly = true;
        System.out.println();
        System.out.println("Usage: java " + _progName + " arguments");
        System.out.println(toString());
    }

    /**
     * @return <i>true</i> if the command line arguments contain a help flag,
     *         <code>-?</code>.
     */
    public boolean isUsageOnly() {
        return _usageOnly;
    }

    /**
     * @param ch
     *            The flag character
     * @return <i>true</i> if the command line arguments contain the specified
     *         flag.
     */
    public boolean isFlag(int ch) {
        return _flags.indexOf(ch) >= 0;
    }

    /**
     * @return A String containing all the flag characters specified in the
     *         command line arguments. For example, if the command line
     *         specifies value flags <code>-a -b -c</code> then this method
     *         returns <code>"abc"</code>.
     */
    public String getFlags() {
        return _flags;
    }

    public boolean booleanValue(final int index) {
        String t = _template[index];
        if (t.startsWith("_flag|")) {
            return isFlag(t.charAt(6));
        } else {
            return false;
        }
    }

    /**
     * @param fieldName
     *            Argument name of a String value specification in the template
     *            array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    public String getStringValue(String fieldName) {
        return _strArgs[lookupName(fieldName)];
    }

    /**
     * @param fieldName
     *            Argument name of an int value specification in the template
     *            array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    public int getIntValue(String fieldName) {
        return (int) _longArgs[lookupName(fieldName)];
    }

    public int intValue(int index) {
        return (int) _longArgs[index];
    }

    /**
     * @param fieldName
     *            Argument name of a long value specification in the template
     *            array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    public long getLongValue(String fieldName) {
        return _longArgs[lookupName(fieldName)];
    }

    public long longValue(final int index) {
        return _longArgs[index];
    }

    public String stringValue(final int index) {
        return _strArgs[index];
    }

    /**
     * Indicate whether the value returned for the specified field is the
     * default value.
     * 
     * @param fieldName
     * @return
     */
    public boolean isDefault(String fieldName) {
        return _isDefault[lookupName(fieldName)];
    }

    private int lookupName(String name) {
        String fieldName1 = name + '|';
        for (int i = 0; i < _template.length; i++) {
            if (_template[i].startsWith(fieldName1))
                return i;
        }
        return -1;
    }

    private void doField(String arg, int position) {
        String type = piece(_template[position], '|', 1);
        String t = piece(type, ':', 0);
        if (arg == null) {
            arg = piece(type, ':', 1);
            _isDefault[position] = true;
        }
        if ("int".equals(t)) {
            long lo = longVal(piece(type, ':', 2), 0);
            long hi = longVal(piece(type, ':', 3), Integer.MAX_VALUE);
            long argInt = longVal(arg, Long.MIN_VALUE);
            if (!_isDefault[position]
                    && (argInt == Long.MIN_VALUE || argInt < lo || argInt > hi || argInt > Integer.MAX_VALUE)) {
                throw new IllegalArgumentException("Invalid argument " + piece(_template[position], '|', 0) + "=" + arg);
            }
            _longArgs[position] = argInt;
        } else if ("long".equals(t)) {
            long lo = longVal(piece(type, ':', 2), 0);
            long hi = longVal(piece(type, ':', 3), Long.MAX_VALUE);
            long argInt = longVal(arg, Long.MIN_VALUE);
            if (!_isDefault[position] && (argInt == Long.MIN_VALUE || argInt < lo || argInt > hi)) {
                throw new IllegalArgumentException("Invalid argument " + piece(_template[position], '|', 0) + "=" + arg);
            }
            _longArgs[position] = argInt;
        } else {
            _strArgs[position] = arg;
        }
    }

    private long longVal(String s, long dflt) {
        if (s.length() == 0)
            return dflt;
        try {
            return Long.parseLong(s.replace(",", ""));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(s + " is not a number");
        }
    }

    private void tab(StringBuilder sb, int count) {
        int last = sb.lastIndexOf(Util.NEW_LINE);
        while (sb.length() - last + 1 < count) {
            sb.append(' ');
        }
    }

    private String piece(String str, char delimiter, int count) {
        int p = -1;
        int q = -1;
        for (int i = 0; i <= count; i++) {
            if (p == str.length())
                return "";
            q = p;
            p = str.indexOf(delimiter, p + 1);
            if (p == -1)
                p = str.length();
        }
        return str.substring(q + 1, p);
    }
}
