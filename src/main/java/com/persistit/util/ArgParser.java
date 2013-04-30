/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

package com.persistit.util;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * A simple command line argument parser that provides primitive type conversion
 * and value checking. An application passes a template and the command line
 * argument array to the constructor of this class. The constructor validates
 * the argument list. Subsequently the application can access specific fields
 * from the command line using the <code>ArgParser</code>'s accessor methods.
 * </p>
 * <p>
 * If the command line includes the argument <code>-?</code> then
 * <code>ArgParser</code> displays a summary of permissible arguments and sets
 * its UsageOnly property to true. The calling application should simply exit if
 * <code>isUsageonly</code> is true for the constructed ArgParser.
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
    private final boolean[] _specified;
    private boolean _usageOnly;
    private final List<String> _unparsed = new ArrayList<String>();

    /**
     * <p>
     * Construct an instance that parses an array of command line arguments
     * according to specifications provided in a template. The supplied template
     * is an array of specification elements.
     * </p>
     * <p>
     * Command line arguments are specified by name, not by position. Each
     * argument must either be a flag in the form <code>-<i>X</i></code> (where
     * <i>X</i> is letter) or a name-value pair in the form
     * <code><i>argname</i>=</i>value</i></code>. The permissible flags and
     * argument names are specified by the array of template strings, each of
     * which must have the form:
     * 
     * <blockquote>
     * 
     * <pre>
     * <code>  _flag|<i>flchar</i>|<i>description</i></code>
     *         or
     * <code>  <i>argname</i>|<i>argtype</i>|<i>description</i></code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * where
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
     * One of:
     * 
     * <pre>
     * <code>int:<i>defaultvalue</i>:<i>lowbound</i>:<i>highbound</i></code>
     * <code>long:<i>defaultvalue</i>:<i>lowbound</i>:<i>highbound</i></code>
     * <code>String:<i>default</i></code>
     * </pre>
     * 
     * </dd>
     * </dl>
     * </p>
     */
    public ArgParser(final String progName, final String[] args, final String[] template) {
        _progName = progName;
        _template = template;
        _strArgs = new String[template.length];
        _longArgs = new long[template.length];
        _specified = new boolean[template.length];

        final StringBuilder flags = new StringBuilder();

        String flagsTemplate = "";
        for (int i = 0; i < template.length; i++) {
            if (template[i].startsWith("_flag|")) {
                flagsTemplate += piece(template[i], '|', 1);
            } else {
                doField(null, i);
            }
        }

        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            if (arg.startsWith("-")) {
                for (int j = 1; j < arg.length(); j++) {
                    final char ch = arg.charAt(j);
                    if (ch == '?') {
                        usage();
                    } else if (flagsTemplate.indexOf(ch) >= 0) {
                        flags.append(ch);
                    } else {
                        _unparsed.add(arg);
                    }
                }
            } else {
                final String fieldName = piece(args[i], '=', 0);
                final int position = lookupName(fieldName);
                if (position < 0) {
                    _unparsed.add(args[i]);
                } else {
                    final String argValue = arg.substring(fieldName.length() + 1);
                    _specified[position] = true;
                    doField(argValue, position);
                }
            }
        }
        _flags = flags.toString();

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < _template.length; i++) {
            final String t = _template[i];
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

    public ArgParser strict() {
        if (!_unparsed.isEmpty()) {
            throw new IllegalArgumentException("Unrecognized arguments: " + _unparsed);
        }
        return this;
    }

    /**
     * Array of arguments that were not parsed by this template. This array may
     * be passed into another ArgParser for further processing.
     * 
     * @return array of argument strings
     */
    public String[] getUnparsedArray() {
        return _unparsed.toArray(new String[_unparsed.size()]);
    }

    /**
     * List of the arguments that were not parsed by this template. The list is
     * modifiable.
     * 
     * @return List of argument strings
     */
    public List<String> getUnparsedList() {
        return _unparsed;
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
    public boolean isFlag(final int ch) {
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

    /**
     * Return the boolean value of argument specified by its index in the
     * template
     * 
     * @param index
     * @return the boolean value for the specified template item
     */
    public boolean booleanValue(final int index) {
        final String t = _template[index];
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
    public String getStringValue(final String fieldName) {
        return _strArgs[lookupName(fieldName)];
    }

    /**
     * @param fieldName
     *            Argument name of an int value specification in the template
     *            array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    public int getIntValue(final String fieldName) {
        return (int) _longArgs[lookupName(fieldName)];
    }

    /**
     * Return the integer value of argument specified by its index in the
     * template
     * 
     * @param index
     * @return the int value for the specified template item
     */
    public int intValue(final int index) {
        return (int) _longArgs[index];
    }

    /**
     * @param fieldName
     *            Argument name of a long value specification in the template
     *            array.
     * @return The corresponding command line argument, or <i>null</i> if the
     *         command line does not name this item.
     */
    public long getLongValue(final String fieldName) {
        return _longArgs[lookupName(fieldName)];
    }

    /**
     * Return the long value of argument specified by its index in the template
     * 
     * @param index
     * @return the long value for the specified template item
     */

    public long longValue(final int index) {
        return _longArgs[index];
    }

    /**
     * Return the String value of argument specified by its index in the
     * template
     * 
     * @param index
     * @return the String value for the specified template item
     */
    public String stringValue(final int index) {
        return _strArgs[index];
    }

    /**
     * Indicate whether the value returned for the specified field is the
     * default value.
     * 
     * @param fieldName
     * @return <code>true</code> if the field contains its default value
     */
    public boolean isSpecified(final String fieldName) {
        return _specified[lookupName(fieldName)];
    }

    private int lookupName(final String name) {
        final String fieldName1 = name + '|';
        for (int i = 0; i < _template.length; i++) {
            if (_template[i].startsWith(fieldName1))
                return i;
        }
        return -1;
    }

    private void doField(String arg, final int position) {
        final String type = piece(_template[position], '|', 1);
        final String t = piece(type, ':', 0);
        if (arg == null) {
            arg = piece(type, ':', 1);
        }
        if ("int".equals(t)) {
            final long lo = longVal(piece(type, ':', 2), 0);
            final long hi = longVal(piece(type, ':', 3), Integer.MAX_VALUE);
            final long argInt = longVal(arg, Long.MIN_VALUE);
            if (_specified[position]
                    && (argInt == Long.MIN_VALUE || argInt < lo || argInt > hi || argInt > Integer.MAX_VALUE)) {
                throw new IllegalArgumentException("Invalid argument " + piece(_template[position], '|', 0) + "=" + arg);
            }
            _longArgs[position] = argInt;
        } else if ("long".equals(t)) {
            final long lo = longVal(piece(type, ':', 2), 0);
            final long hi = longVal(piece(type, ':', 3), Long.MAX_VALUE);
            final long argInt = longVal(arg, Long.MIN_VALUE);
            if (_specified[position] && (argInt == Long.MIN_VALUE || argInt < lo || argInt > hi)) {
                throw new IllegalArgumentException("Invalid argument " + piece(_template[position], '|', 0) + "=" + arg);
            }
            _longArgs[position] = argInt;
        } else {
            _strArgs[position] = arg;
        }
    }

    private long longVal(final String s, final long dflt) {
        if (s.length() == 0)
            return dflt;
        try {
            return Long.parseLong(s.replace(",", ""));
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(s + " is not a number");
        }
    }

    private void tab(final StringBuilder sb, final int count) {
        final int last = sb.lastIndexOf(Util.NEW_LINE);
        while (sb.length() - last + 1 < count) {
            sb.append(' ');
        }
    }

    private String piece(final String str, final char delimiter, final int count) {
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
