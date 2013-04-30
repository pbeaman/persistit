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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Select Volumes, Trees or Keys given a pattern string. The CLI utilities use
 * this to select Volumes and/or Trees. Syntax:
 * 
 * <pre>
 * <i>volpattern</i>[:<i>treepattern</i>[<i>keyfilter</i>],...
 * </pre>
 * 
 * where <i>volpattern</i> and <i>treepattern</i> are pattern strings that use
 * "*" and "?" as multi-character and single-character wild-cards.
 * (Alternatively, if the <code>regex</code> flag is set, these are true regular
 * expressions.) Example:
 * 
 * <code><pre>
 * v1:*index*{"a"-"f"},*data/*
 * </pre></code>
 * 
 * selects all trees in volume named "v1" having names containing the substring
 * "index", and all tress in all values having names that end with "data". For
 * trees selected in volume v1, there is a keyfilter that specifies keys
 * starting with letters 'a' through 'f'.
 * <p />
 * The {@link #parseSelector(String, boolean, char)} method takes a quote
 * character, normally '\\', that may be used to quote the meta characters in
 * patterns, commas and colons.
 * 
 * @author peter
 */
public class TreeSelector {

    private final static char WILD_MULTI = '*';
    private final static char WILD_ONE = '?';
    private final static char COMMA = ',';
    private final static char COLON = ':';
    private final static char LBRACE = '{';
    private final static char RBRACE = '}';
    private final static char NUL = (char) 0;
    private final static String CAN_BE_QUOTED = "*?,";
    private final static String REGEX_QUOTE = "^$*+?()[].";

    /**
     * Constraints on volume name, tree name and/or key.
     */
    private static class Selector {
        Pattern _vpattern;
        Pattern _tpattern;
        KeyFilter _keyFilter;

        private boolean isNull() {
            return _vpattern == null && _tpattern == null && _keyFilter == null;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(_vpattern);
            if (_tpattern != null) {
                sb.append(COLON);
                sb.append(_tpattern);
                if (_keyFilter != null) {
                    sb.append(_keyFilter);
                }
            }
            return sb.toString();
        }

    }

    private static enum State {
        V, T, K, C
    }

    /**
     * Create a <code>TreeSelector</code> based on the supplied parameters.
     * 
     * @param spec
     *            The specification string
     * @param regex
     *            <code>true</code> if the specification string is a Regex
     *            expression, <code>false</code> if it simply uses '*' and '?'
     *            as wildcards
     * @param quote
     *            meta-character to quote the next character, typically '\'
     * @return the <code>TreeSelector</code>
     */

    public static TreeSelector parseSelector(final String spec, final boolean regex, final char quote) {
        final TreeSelector treeSelector = new TreeSelector();
        if ("*".equals(spec)) {
            return treeSelector;
        }
        Selector s = new Selector();
        State state = State.V;
        final StringBuilder sb = new StringBuilder();
        boolean quoted = false;
        final int size = spec.length() + 1;
        for (int index = 0; index < size; index++) {
            final char c = index + 1 < size ? spec.charAt(index) : (char) 0;
            if (quoted) {
                sb.append(c);
                quoted = false;
            } else if (c == quote && index < size - 2 && CAN_BE_QUOTED.indexOf(spec.charAt(index + 1)) > 0) {
                quoted = true;
            } else {
                switch (state) {
                case V:
                    if (c == NUL && sb.length() == 0) {
                        break;
                    }
                    if (c == WILD_MULTI && !regex) {
                        sb.append(".*");
                    } else if (c == WILD_ONE && !regex) {
                        sb.append(".");
                    } else if (!regex && REGEX_QUOTE.indexOf(c) != -1) {
                        sb.append('\\');
                        sb.append(c);
                    } else if (c == COLON || c == COMMA || c == NUL) {
                        s._vpattern = Pattern.compile(sb.toString());
                        sb.setLength(0);
                        if (c == COLON) {
                            state = State.T;
                        } else {
                            treeSelector._terms.add(s);
                            s = new Selector();
                            state = State.V;
                        }
                    } else {
                        sb.append(c);
                    }
                    break;
                case T:
                    if (c == WILD_MULTI && !regex) {
                        sb.append(".*");
                    } else if (c == WILD_ONE && !regex) {
                        sb.append(".");
                    } else if (!regex && REGEX_QUOTE.indexOf(c) != -1) {
                        sb.append('\\');
                        sb.append(c);
                    } else if (c == LBRACE || c == COMMA || c == NUL) {
                        s._tpattern = Pattern.compile(sb.toString());
                        sb.setLength(0);
                        if (c == LBRACE) {
                            state = State.K;
                            sb.append(c);
                        } else {
                            treeSelector._terms.add(s);
                            s = new Selector();
                            state = State.V;
                        }
                    } else {
                        sb.append(c);
                    }
                    break;
                case K:
                    sb.append(c);
                    if (c == RBRACE || c == NUL) {
                        s._keyFilter = new KeyParser(sb.toString()).parseKeyFilter();
                        treeSelector._terms.add(s);
                        s = new Selector();
                        sb.setLength(0);
                        state = State.C;
                    }
                    break;
                case C:
                    if (c == COMMA || c == NUL) {
                        state = State.V;
                    } else {
                        throw new IllegalArgumentException("at index=" + index);
                    }
                }
            }
        }
        return treeSelector;
    }

    private final List<Selector> _terms = new ArrayList<Selector>();

    public boolean isSelectAll() {
        return _terms.isEmpty();
    }

    public int size() {
        return _terms.size();
    }

    public boolean isSelected(final Volume volume) {
        return isVolumeNameSelected(volume.getName());
    }

    public boolean isSelected(final Tree tree) {
        return isTreeNameSelected(tree.getVolume().getName(), tree.getName());
    }

    public boolean isVolumeNameSelected(final String volumeName) {
        if (_terms.isEmpty()) {
            return true;
        }
        for (final Selector selector : _terms) {
            if (selector._vpattern == null || selector._vpattern.matcher(volumeName).matches()) {
                return true;
            }
        }
        return false;
    }

    public boolean isVolumeOnlySelection(final String volumeName) {
        if (_terms.isEmpty()) {
            return true;
        }
        for (final Selector selector : _terms) {
            if ((selector._vpattern == null || selector._vpattern.matcher(volumeName).matches())
                    && selector._tpattern == null && selector._keyFilter == null) {
                return true;
            }
        }
        return false;
    }

    public boolean isTreeNameSelected(final String volumeName, final String treeName) {
        if (_terms.isEmpty()) {
            return true;
        }
        for (final Selector selector : _terms) {
            if ((selector._vpattern == null || selector._vpattern.matcher(volumeName).matches())
                    && (selector._tpattern == null || selector._tpattern.matcher(treeName).matches())) {
                return true;
            }
        }
        return false;
    }

    public KeyFilter keyFilter(final String volumeName, final String treeName) {
        KeyFilter kf = null;
        for (final Selector selector : _terms) {
            if ((selector._vpattern == null || selector._vpattern.matcher(volumeName).matches())
                    && (selector._tpattern == null || selector._tpattern.matcher(treeName).matches())) {
                if (kf == null) {
                    kf = selector._keyFilter;
                    if (kf == null) {
                        kf = new KeyFilter();
                    }
                } else {
                    throw new IllegalStateException("Non-unique KeyFilters for tree " + volumeName + "/" + treeName);
                }
            }
        }
        return kf;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final Selector ts : _terms) {
            if (sb.length() > 0) {
                sb.append(COMMA);
            }
            sb.append(ts.toString());
        }
        return sb.toString();
    }

}
