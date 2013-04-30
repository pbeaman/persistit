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

package com.persistit;

import com.persistit.encoding.CoderContext;
import com.persistit.exception.MissingKeySegmentException;
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * <p>
 * Specifies a subset of all possible keys values. A <code>KeyFilter</code> can
 * be used with the {@link Exchange#traverse(Key.Direction, KeyFilter, int)}
 * method to restrict the set of key values within a Persistit <code>Tree</code>
 * that will actually be traversed.
 * </p>
 * <p>
 * A <code>KeyFilter</code> provides two primary methods:
 * <ul>
 * <li>
 * {@link #selected(Key)} indicates whether the value of the specified
 * <code>Key</code> is a member of the subset specified by this filter, and</li>
 * <li>
 * {@link #next(Key, Key.Direction)} modifies the <code>Key</code> to the next
 * larger or smaller key value that lies within the range specified by this
 * filter.</li>
 * </ul>
 * These methods permit efficient traversal of a filtered subset of keys within
 * a <code>Tree</code>.
 * </p>
 * <p>
 * <h3>KeyFilter Terms</h3>
 * A <code>KeyFilter</code> consists of an array of one or more <i>terms</i>.
 * Each term corresponds to one segment in a key value that will be selected or
 * excluded by this <code>KeyFilter</code>. The <i>K</i>th term in the list
 * applies to the <i>K</i>th segment of a key.
 * </p>
 * <p>
 * There are three kinds of term:
 * <dl>
 * <dt>SimpleTerm</dt>
 * <dd>Represents a single segment value. To match a SimpleTerm, the value of
 * the key segment in the corresponding position must exactly match the
 * SimpleTerm's value.</dd>
 * <dt>RangeTerm</dt>
 * <dd>Represents a range of values. To match a RangeTerm, the value of the key
 * segment in the corresponding position must fall between the end points of the
 * range according to the <a href="Key.html#_keyOrdering">key ordering
 * specification</a>. RangeTerms can be defined to include or exclude either end
 * point.</dd>
 * <dt>OrTerm</dt>
 * <dd>Represents alternatives. Each alternative is itself a SimpleTerm or a
 * RangeTerm. To match an OrTerm, the value of the key segment in the
 * corresponding position must match one of the alternative terms associated
 * with the OrTerm.</dd>
 * </p>
 * <p>
 * The static methods {@link #simpleTerm(Object)},
 * {@link #rangeTerm(Object, Object)},
 * {@link #rangeTerm(Object, Object, CoderContext)},
 * {@link #rangeTerm(Object, Object, boolean, boolean)},
 * {@link #rangeTerm(Object, Object, boolean, boolean, CoderContext)}, and
 * {@link #orTerm} produce these various kinds of <code>Term</code>s
 * <p>
 * For example, consider a key consisting of three segments: a last name, a
 * first name and a person ID number for a person. Such a key value might be
 * constructed with code such as this:
 * 
 * <code><pre>
 *     key.clear().append("McDonald").append("Bob").append(12345);
 * </pre></code>
 * 
 * Suppose we now want to enumerate all members of a tree having keys of this
 * form with last names falling between "M" and "Q", and first names equal to
 * either "Alice" or "Bob". The following code constructs a
 * <code>KeyFilter</code> for this purpose:
 * 
 * <code><pre>
 *     KeyFilter keyFilter = new KeyFilter();
 *     keyFilter = keyFilter.append(KeyFilter.rangeTerm("M", "Q"));
 *     keyFilter = keyFilter.append(KeyFilter.orTerm(new KeyFilter.Term[]{
 *         KeyFilter.simpleTerm("Alice"), KeyFilter.simpleTerm("Bob"))});
 * </pre></code>
 * 
 * The first term specifies a range that includes any last name that sorts
 * alphabetically between "M" and "Q" (inclusively). The second term is an
 * OrTerm that selects the first names "Alice" or "Bob".
 * </p>
 * <p>
 * A RangeTerm optionally specifies whether the end-points are inclusive. For
 * example, the term
 * 
 * <code><pre>
 *     KeyFilter.rangeTerm("Jones", "Smith", true, false)
 * </pre></code>
 * 
 * includes the name "Jones" and all names that follow up to, but not including,
 * "Smith". If unspecified, the end-points of the range are included.
 * </p>
 * <p>
 * <h3>Minimum and Maximum Depth</h3>
 * A <code>KeyFilter</code> may also specify <i>minimum depth</i>, <i>maximum
 * depth</i>, or both. These values control the number of segments that must be
 * present in key value in order for it to be selected. A <code>KeyFilter</code>
 * will select a key only if the number of segments in the key lies between the
 * minimum depth and the maximum depth, inclusive.
 * </p>
 * <p>
 * <a name="_stringRepresentation" />
 * <h3>String Representation</h3>
 * The {@link #toString()} method returns a canonical String representation of
 * the current terms for this <code>KeyFilter</code>. For example, the string
 * representation of the filter constructed in above is
 * 
 * <code><pre>
 *     {"M":"Q",{"Alice","Bob"}}
 * </pre></code>
 * 
 * You can construct a <code>KeyFilter</code> from its string representation
 * with the {@link KeyParser#parseKeyFilter} method. For example, the following
 * code generates an equivalent <code>KeyFilter</code>:
 * 
 * <code><pre>
 *     KeyParser parser = new KeyParser("{\"M\":\"Q\",{\"Alice\",\"Bob\"}};
 *     KeyFilter filter = parser.parseKeyFilter();
 * </pre></code>
 * 
 * As a convenience, the constructor {@link #KeyFilter(String)} automatically
 * creates and invokes a <code>KeyParser</code> to create a
 * <code>KeyFilter</code> from its string representation.
 * </p>
 * <p>
 * Following is an informal grammar for the string representation of a key
 * filter. See <a href="Key.html#_stringRepresentation">string
 * representation</a> in {@link Key} for information on how to specify a key
 * segment value.
 * 
 * <code><pre>
 *    keyFilter ::= '{' termElement [',' termElement]... '}'
 *    termElement ::= [ '&gt;' ] term [ '&lt;' ]
 *    term ::= segment | range | qualifiedRange | orTerm
 *    segment ::= see <a href="Key.html#_stringRepresentation">segment</a>
 *    range ::= segment ':' segment | ':' segment | segment ':'
 *    qualifiedRange = ('(' | '[') range (')' | ']')
 *    orTerm ::= '{' term [',' term ]...'}'
 * </pre></code>
 * 
 * A <i>range</i> may omit either the starting segment value or the ending
 * segment value. When the starting segment value is omitted, the range starts
 * before the first possible key value, and when the ending segment value is
 * omitted, the range ends after the last possible key value. Thus the range
 * specification
 * 
 * <code><pre>
 *   {"Smith":}
 * </pre></code>
 * 
 * include every key with a first segment value of "Smith" or above. Similarly,
 * 
 * <code><pre>
 *   {:"Smith"}
 * </pre></code>
 * 
 * includes all keys up to and including "Smith".
 * </p>
 * <p>
 * A <i>qualifiedRange</i> allows you to specify whether the end-points of a
 * range are included or excluded from the selected subset. A square bracket
 * indicates that the end-point is included, while a parenthesis indicates that
 * it is excluded. For example
 * 
 * <code><pre>
 *   {("Jones":"Smith"]}
 * </pre></code>
 * 
 * does not include "Jones" but does include "Smith". An unqualified
 * <i>range</i> specification such as
 * 
 * <code><pre>
 *   {"Jones":"Smith"}
 * </pre></code>
 * 
 * includes both end-points. It is equivelent to
 * 
 * <code><pre>
 *   {["Jones":"Smith"]}
 * </pre></code>
 * 
 * </p>
 * <p>
 * Within the string representation of a <code>KeyFilter</code> at most one term
 * element may specify the prefix "&gt;" (greater-than sign), and at most one
 * term element may specify the suffix "&lt;" (less-than sign). These denote the
 * minimum and maximum depths of the <code>KeyFilter</code>, respectively. The
 * minimum depth is the count of term elements up to and including the term
 * marked with a "&gt;" and the maximum depth is the count of terms up to and
 * including the term marked with a "&gt;". For example, in the
 * <code>KeyFilter</code> represented by the string
 * 
 * <code><pre>
 *   {*,>100:200,*<}
 * </pre></code>
 * 
 * the minimum depth is 2 and the maximum depth is 3.
 * </p>
 * <h3>Building KeyFilters by Appending Terms</h3>
 * <p>
 * A <code>KeyFilter</code> is immutable. The methods {@link #append(KeyFilter)}, {@link #append(KeyFilter.Term)}, {@link #append(KeyFilter.Term[])}, create
 * new <code>KeyFilter</code>s with additional terms supplied by the supplied
 * <code>KeyFilter</code>, <code>Term</code> or array of <code>Term</code>s. The
 * {@link #limit} method creates a new <code>KeyFilter</code> with modified
 * minimum and maximum depth values. Each of these methods returns a new
 * <code>KeyFilter</code> which results from combining the original
 * <code>KeyFilter</code> with the supplied information.
 * </p>
 * <h3>
 * Formal Specification of the {@link #next(Key, Key.Direction)} Method</h3>
 * <p>
 * A KeyFilter defines a subset of the the set of all Key values: the
 * {@link Exchange#traverse(Key.Direction, KeyFilter, int)} method returns only
 * values in this subset. The following definitions are used in describing the
 * behavior of the {@link #next(Key, Key.Direction)} method.
 * <dl>
 * <dt>Range</dt>
 * <dd>Let S be the ordered set of all possible key values. (Though large, this
 * set is finite because a Keys have finite length.) The <i>range</i> R is the
 * subset of keys in S selected by this KeyFilter.</dd>
 * <dt>Adjacent</dt>
 * <dd>Two keys K1 and K2 in this set are <i>adjacent</i> if K1 != K2 and there
 * exists no other key value K such that K1 &lt; K &lt; K2. The terms
 * <i>left-adjacent</i> and <i>right-adjacent</i> describe the precedence: K1 is
 * left-adjacent to K2; K2 is right-adjacent to K1</dd>
 * <dt>Contiguous</dt>
 * <dd>Let C be a subset of R. Let Kmin and Kmax be the smallest and largest
 * keys in C, respectively. Then C is <i>contiguous</i> if there is no key K not
 * in R where Kmin &lt; K &lt; Kmax.</dd>
 * <dt>Direction</dt>
 * <dd>The {@link Key.Direction} is supplied to the
 * {@link Exchange#traverse(Key.Direction, KeyFilter, int)} method to control
 * navigation. There are five possible values:
 * <ul>
 * <li>LT: the result key must be strictly less than the supplied key.</li>
 * <li>LTEQ: the result key must be less than or equal to the supplied key.</li>
 * <li>EQ: the result key must be equal to the supplied key.</li>
 * <li>GTEQ: the result key must be greater than or equal to the supplied key.</li>
 * <li>GT: the result key must be strictly greater than the supplied key.</li>
 * </ul>
 * LT and GT are called <i>exclusive</i> because the result of
 * <code>traverse</code> excludes the supplied key. LTEQ and GTEQ are
 * <i>inclusive</i>.
 * </dl>
 * </p>
 * <p>
 * A KeyFilter defines the range R as zero or more contiguous subsets of S. The
 * <code>next</code> method is used to assist the <code>traverse</code> method
 * by skipping efficiently over keys that are not in contained in R. Given a
 * {@link Key} K and a {@link Key.Direction} D, the method behaves as follows:
 * </p>
 * <p>
 * <ol>
 * <li>
 * If K is in R (i.e., is <i>selected</i>) and either D is inclusive, or there
 * exists a key value K' in R that is adjacent to K (where K' is larger than K
 * if D is GT, or smaller than K if D is LT), then return <code>true</code>
 * without modifying the K.</li>
 * <li>
 * Otherwise attempt to modify K to a new value K' which is either adjacent to
 * or in the next contiguous subset of R, and return <code>true</code> to
 * indicate that more keys exist in the range. Specifically, for each Direction
 * D, K' is defined as follows:
 * <ul>
 * <li>LT: find the largest key J in R where J &lt; K. Then K' is the key
 * right-adjacent to J.</li>
 * <li>LTEQ: find the largest key J in R where J &lt; K. Then K' is J.</li>
 * <li>EQ: there is no K'</li>
 * <li>GTEQ: find the smallest key J in R where J &gt; K. Then K' is J.</li>
 * <li>GT: find the smallest key J in R where J &lt; K. Then K' is the key
 * left-adjacent to J.</li>
 * </ul>
 * </li>
 * <li>
 * Otherwise, if there is no key K' that satisfies the requirements of #2,
 * return <code>false</code> to indicate that the range has been exhausted.</li>
 * </ol>
 * </p>
 * 
 * @version 1.0
 */
public class KeyFilter {
    /**
     * A {@link KeyFilter.Term} that matches all values.
     */
    public final static Term ALL = new SimpleTerm(null);

    private Term[] _terms;
    private int _minDepth = 0;
    private int _maxDepth = Integer.MAX_VALUE;

    /**
     * Flag for filters that are guaranteed to be a subset of key range. For
     * example, as created by {@link #KeyFilter(Key, int, int)}. Allows
     * optimized traversal.
     */
    private int _keyPrefixByteCount = 0;
    private boolean _isKeyPrefixFilter = false;

    /**
     * Constructs an empty <code>KeyFilter</code>. This <code>KeyFilter</code>
     * implicitly selects all key values.
     */
    public KeyFilter() {
        _terms = new Term[0];
    }

    /**
     * Constructs a <code>KeyFilter</code> from its <a
     * href="#_stringRepresentation"> string representation</a>.
     * 
     * @param string
     *            The string representation
     * 
     * @throws IllegalArgumentException
     *             if the string is not valid
     */
    public KeyFilter(final String string) {
        final KeyParser parser = new KeyParser(string);
        final KeyFilter filter = parser.parseKeyFilter();
        if (filter == null) {
            throw new IllegalArgumentException("Invalid KeyFilter expression");
        } else {
            _terms = filter._terms;
            _minDepth = filter._minDepth;
            _maxDepth = filter._maxDepth;
        }
    }

    /**
     * Constructs a <code>KeyFilter</code> that selects the subset of all keys
     * which are equal to, <a href="Key.html#_keyChildren">logical children</a>
     * of, or logical ancestors of the supplied <code>Key</code>.
     * 
     * @param key
     *            The <code>Key</code>
     */
    public KeyFilter(final Key key) {
        this(key, 0, Integer.MAX_VALUE);
    }

    /**
     * Constructs a <code>KeyFilter</code> that selects the subset of all key
     * values that are equal to, <a href="Key.html#_keyChildren">logical
     * children</a> of, or logical ancestors of the supplied <code>Key</code>,
     * and whose depth is greater than or equal to the supplied minimum depth
     * and less than or equal to the supplied maximum depth. Suppose the
     * supplied <code>key</code> value has <i>M</i> segments and some other
     * <code>Key</code> value <i>K</i> has <i>N</i> segments. Then <i>K</i> is a
     * member of the subset selected by this <code>KeyFilter</code> if and only
     * if <i>N</i>&gt;=<code>minDepth</code> and <i>N</i>&lt;=
     * <code>maxDepth</code> and each of the first min(<i>M</i>, <i>N</i>)
     * segments match.
     * 
     * @param key
     *            The <code>Key</code>
     * 
     * @param minDepth
     *            The minimum depth
     * 
     * @param maxDepth
     *            The maximum depth
     * 
     */
    public KeyFilter(final Key key, final int minDepth, final int maxDepth) {
        checkLimits(minDepth, maxDepth);
        int size = 0;
        int index = 0;
        if (key != null) {
            _terms = new Term[key.getDepth()];
            size = key.getEncodedSize();
        }
        if (key != null && size != 0) {
            _isKeyPrefixFilter = true;
            for (int level = 0;; level++) {
                final int previous = index;
                index = key.nextElementIndex(previous);
                if (index < 0)
                    break;
                if (level <= minDepth)
                    _keyPrefixByteCount += (index - previous);
                final byte[] bytes = new byte[index - previous];
                System.arraycopy(key.getEncodedBytes(), previous, bytes, 0, bytes.length);
                _terms[level] = new SimpleTerm(bytes);
            }
        }
        _minDepth = minDepth;
        _maxDepth = maxDepth;
    }

    /**
     * Constructs a <code>KeyFilter</code> that selects the subset of all keys
     * whose segments are selected by the corresponding <code>Term</code>s of
     * the supplied array. Suppose a Key <i>K</i> has <i>N</i> segments and the
     * supplied array of <code>terms</code> has length <i>M</i>. Then <i>K</i>
     * is a member of the key value subset selected by this
     * <code>KeyFilter</code> if and only if each of the first min(<i>M</i>,
     * <i>N</i>) segments of <i>K</i> is selected by the corresponding member of
     * the <code>terms</code> array.
     * 
     * @param terms
     */
    public KeyFilter(final Term[] terms) {
        this(terms, 0, Integer.MAX_VALUE);
    }

    /**
     * Constructs a <code>KeyFilter</code> that selects the subset of all keys
     * whose segments are selected by the corresponding <code>Term</code>s of
     * the supplied array and whose depth is greater than or equal to the
     * supplied minimum depth and less than or equal to the supplied maximum
     * depth. Suppose some Key <i>K</i> has <i>N</i> segments and the supplied
     * array of <code>terms</code> has length <i>M</i>. Then <i>K</i> is a
     * member of the key value subset selected by this <code>KeyFilter</code> if
     * and only if and only if <i>N</i>&gt;=<code>minDepth</code> and
     * <i>N</i>&lt;=<code>maxDepth</code> and each of the first min(<i>M</i>,
     * <i>N</i>) segments of <i>K</i> is selected by the corresponding member of
     * the <code>terms</code> array.
     * 
     * @param terms
     *            The <code>Term</code> array
     * 
     * @param minDepth
     *            The minimum depth
     * 
     * @param maxDepth
     *            The maximum depth
     * 
     */
    public KeyFilter(final Term[] terms, final int minDepth, final int maxDepth) {
        checkLimits(minDepth, maxDepth);
        _terms = terms;
        _minDepth = minDepth;
        _maxDepth = maxDepth;
    }

    /**
     * Constructs and returns a new <code>KeyFilter</code> in which the terms of
     * the supplied <code>filter</code> are appended to the array of terms
     * already present in this <code>KeyFilter</code>. In addition, the minimum
     * and maximum depths of the newly created <code>KeyFilter</code> are
     * computed from the supplied <code>filter</code> value. Let M be the number
     * of terms in this <code>KeyFilter</code>. The the minimum and maximum
     * depth parameters for the newly created <code>KeyFilter</code> will be
     * <code>filter.getMinimumDepth()+</code>M and
     * <code>filter.getMaximumDepth()+</code>M, respectively.
     * 
     * @param filter
     *            The <code>KeyFilter</code> to append
     * 
     * @return The newly constructed <code>KeyFilter</code>.
     */
    public KeyFilter append(final KeyFilter filter) {
        final KeyFilter newFilter = new KeyFilter(merge(_terms, filter._terms));

        final int size = _terms.length;

        newFilter._minDepth = filter._minDepth + size;

        if (Integer.MAX_VALUE - size > filter._maxDepth) {
            newFilter._maxDepth = filter._maxDepth + size;
        }

        return newFilter;
    }

    /**
     * Constructs and returns a new <code>KeyFilter</code> in which the supplied
     * <code>term</code> is appended to the end of the array of terms in the
     * current <code>KeyFilter</code>.
     * 
     * @param term
     *            The <code>Term</code> to append
     * 
     * @return The newly constructed <code>KeyFilter</code>
     */
    public KeyFilter append(final Term term) {
        final Term[] newTerms = new Term[_terms.length + 1];
        System.arraycopy(_terms, 0, newTerms, 0, _terms.length);
        newTerms[_terms.length] = term;
        return new KeyFilter(newTerms);
    }

    /**
     * Constructs and returns a new <code>KeyFilter</code> in which the supplied
     * <code>terms</code> are appended to the array of terms in the current
     * <code>KeyFilter</code>.
     * 
     * @param terms
     *            The array of <code>Term</code> to append
     * 
     * @return The newly constructed <code>KeyFilter</code>
     */
    public KeyFilter append(final Term[] terms) {
        final Term[] newTerms = merge(_terms, terms);
        return new KeyFilter(newTerms);
    }

    /**
     * Constructs and returns a new <code>KeyFilter</code> in which the minimum
     * and maximum depth are set to the supplied values.
     * 
     * @param minDepth
     *            The minimum depth
     * 
     * @param maxDepth
     *            The maximum depth
     * 
     * @return The newly constructed <code>KeyFilter</code>.
     */
    public KeyFilter limit(final int minDepth, final int maxDepth) {
        checkLimits(minDepth, maxDepth);
        final KeyFilter newFilter = new KeyFilter(_terms);
        newFilter._minDepth = minDepth;
        newFilter._maxDepth = maxDepth;
        return newFilter;
    }

    private void checkLimits(final int minDepth, final int maxDepth) {
        if (minDepth < 0) {
            throw new IllegalArgumentException("minDepth (" + minDepth + ") must be >= 0");
        }
        if (minDepth > maxDepth) {
            throw new IllegalArgumentException("minDepth (" + minDepth + ") must be <= maxDepth (" + maxDepth + ")");
        }
    }

    private Term[] merge(final Term[] a, final Term[] b) {
        final int sizeA = a == null ? 0 : a.length;
        final int sizeB = b == null ? 0 : b.length;
        if (sizeA == 0 && b != null)
            return b;
        if (sizeB == 0 && a != null)
            return a;
        final Term[] terms = new Term[sizeA + sizeB];
        if (sizeA > 0)
            System.arraycopy(a, 0, terms, 0, sizeA);
        if (sizeB > 0)
            System.arraycopy(b, 0, terms, sizeA, sizeB);
        return terms;
    }

    /**
     * <p>
     * Specifies criteria for selecting one segment of a <code>Key</code> value.
     * This abstract class has three concrete subclasses,
     * <code>SimpleTerm</code>, <code>RangeTerm</code> and <code>OrTerm</code>
     * as described for {@link KeyFilter} .
     * </p>
     * <p>
     * Use the static factory methods {@link KeyFilter#simpleTerm(Object)},
     * {@link KeyFilter#rangeTerm(Object, Object)},
     * {@link KeyFilter#rangeTerm(Object, Object, CoderContext)},
     * {@link KeyFilter#rangeTerm(Object, Object, boolean, boolean)},
     * {@link KeyFilter#rangeTerm(Object, Object, boolean, boolean, CoderContext)}
     * , and {@link KeyFilter#orTerm} to create instances of <code>Term</code>.
     */
    public static abstract class Term {
        protected int _hashCode = -1;

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <code>Term</code>.
         * 
         * @return A canonical String representation
         */
        @Override
        public String toString() {
            return toString(null);
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <code>Term</code> using the supplied
         * <code>context</code>. The <code>context</code> is used only if the
         * segment value for a <code>SimpleTerm</code>, or values for a
         * <code>RangeTerm</code> are members of a class with a registered
         * {@link com.persistit.encoding.KeyCoder} the uses a
         * {@link com.persistit.encoding.CoderContext}.
         * 
         * @param context
         *            A <code>CoderContext</code> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <code>Term</code>. May be <code>null</code>.
         * 
         * @return A canonical String representation
         */
        public String toString(final CoderContext context) {
            final StringBuilder sb = new StringBuilder();
            toString(context, sb);
            return sb.toString();
        }

        abstract void toString(CoderContext context, StringBuilder sb);

        abstract boolean selected(byte[] keyBytes, int offset, int length);

        abstract boolean atEdge(byte[] keyBytes, int offset, int length, boolean forward);

        abstract boolean forward(Key key, int offset, int length);

        abstract boolean backward(Key key, int offset, int length);

        abstract byte[] leftBytes();

        abstract byte[] rightBytes();
    }

    static class SimpleTerm extends Term {
        private final byte[] _itemBytes;

        SimpleTerm(final byte[] bytes) {
            _itemBytes = bytes;
        }

        @Override
        public int hashCode() {
            if (_hashCode == -1) {
                _hashCode = byteHash(_itemBytes) & 0x7FFFFFFF;
            }
            return _hashCode;
        }

        @Override
        public boolean equals(final Object object) {
            if (object instanceof SimpleTerm) {
                final SimpleTerm t = (SimpleTerm) object;
                return compare(_itemBytes, t._itemBytes) == 0;
            }
            return false;
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <code>Term</code>, using the supplied
         * CoderContext when necessary.
         * 
         * @param context
         *            A <code>CoderContext</code> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <code>Term</code>. May be <code>null</code>.
         * 
         * @return A canonical String representation
         */
        @Override
        public void toString(final CoderContext context, final StringBuilder sb) {
            if (this == ALL) {
                sb.append("*");
            } else {
                final Key workKey = new Key((Persistit) null);
                appendDisplayableKeySegment(workKey, sb, _itemBytes, context, false, false);
            }
        }

        @Override
        boolean selected(final byte[] keyBytes, final int offset, final int length) {
            if (length == 0) {
                return false;
            }
            if (this == ALL) {
                return true;
            }
            if (length != _itemBytes.length) {
                return false;
            }
            for (int index = 0; index < length; index++) {
                if (_itemBytes[index] != keyBytes[offset + index])
                    return false;
            }
            return true;
        }

        @Override
        boolean atEdge(final byte[] keyBytes, final int offset, final int length, final boolean forward) {
            return this == ALL ? false : true;
        }

        @Override
        boolean forward(final Key key, final int offset, final int length) {
            if (this == ALL) {
                if (length == 0) {
                    key.setEncodedSize(offset);
                    key.appendBefore();
                } else if (!key.isSpecial()) {
                    key.nudgeRight();
                }
                return true;
            }

            final byte[] keyBytes = key.getEncodedBytes();

            final int compare = compare(keyBytes, offset, length, _itemBytes, 0, _itemBytes.length);

            if (compare < 0) {
                System.arraycopy(_itemBytes, 0, keyBytes, offset, _itemBytes.length);
                key.setEncodedSize(offset + _itemBytes.length);
                return true;
            } else {
                return false;
            }
        }

        @Override
        boolean backward(final Key key, final int offset, final int length) {
            if (this == ALL) {
                if (length == 0) {
                    key.setEncodedSize(offset);
                    key.appendAfter();
                } else if (!key.isSpecial()) {
                    key.nudgeLeft();
                }
                return true;
            }

            final byte[] keyBytes = key.getEncodedBytes();

            final int compare = length == 0 ? 1 : compare(keyBytes, offset, length, _itemBytes, 0, _itemBytes.length);

            if (compare > 0) {
                System.arraycopy(_itemBytes, 0, keyBytes, offset, _itemBytes.length);
                key.setEncodedSize(offset + _itemBytes.length);
                return true;
            } else {
                return false;
            }
        }

        @Override
        byte[] leftBytes() {
            return _itemBytes;
        }

        @Override
        byte[] rightBytes() {
            return _itemBytes;
        }
    }

    /**
     * Represents a restriction on an individual segment value within a key. A
     * term may specify a single value, a range of values, or an array of terms.
     * This class does not have a public constructor. Use the static factory
     * methods {@link KeyFilter#simpleTerm(Object)},
     * {@link KeyFilter#rangeTerm(Object, CoderContext)},
     * {@link KeyFilter#rangeTerm(Object, Object)},
     * {@link KeyFilter#rangeTerm(Object, Object, boolean, boolean, CoderContext)}
     * , and {@link KeyFilter#rangeTerm(Object, Object, CoderContext)} to create
     * appropriate <code>Term</code> instances.
     */
    static class RangeTerm extends Term {
        private final byte[] _itemFromBytes;
        private final byte[] _itemToBytes;
        private final boolean _leftInclusive;
        private final boolean _rightInclusive;

        RangeTerm(final byte[] leftBytes, final byte[] rightBytes, final boolean leftInclusive,
                final boolean rightInclusive) {
            _itemFromBytes = leftInclusive ? leftBytes : nudgeRight(leftBytes);
            _itemToBytes = rightInclusive ? rightBytes : nudgeLeft(rightBytes);
            _leftInclusive = leftInclusive;
            _rightInclusive = rightInclusive;
        }

        /**
         * Indicates whether two <code>Term</code> instances are equal. They are
         * equal if the segment values delimiting their ranges and their start-
         * and end-point inclusion settings are the same.
         * 
         * @param object
         *            The Object to be compared
         * 
         * @return <code>true</code> if the supplied object is equal to this
         *         <code>Term</code>; otherwise <code>false</code>.
         */
        @Override
        public boolean equals(final Object object) {
            if (object instanceof RangeTerm) {
                final RangeTerm t = (RangeTerm) object;
                final boolean result = compare(_itemFromBytes, t._itemFromBytes) == 0
                        && compare(_itemToBytes, t._itemToBytes) == 0;
                return result;
            }
            return false;
        }

        /**
         * Computes a hash code for this <code>Term</code>.
         * 
         * @return The hash code
         */
        @Override
        public int hashCode() {
            if (_hashCode == -1) {
                _hashCode = (byteHash(_itemFromBytes) ^ byteHash(_itemToBytes)) & 0x7FFFFFFF;
            }
            return _hashCode;
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <code>Term</code>, using the supplied
         * CoderContext when necessary.
         * 
         * @param context
         *            A <code>CoderContext</code> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <code>Term</code>. May be <code>null</code>.
         * 
         * @return A canonical String representation
         */
        @Override
        public void toString(final CoderContext context, final StringBuilder sb) {
            final Key workKey = new Key((Persistit) null);
            final boolean allInclusive = _leftInclusive && _rightInclusive;
            if (!allInclusive) {
                sb.append(_leftInclusive ? "[" : "(");
            }
            final byte[] from = _leftInclusive ? _itemFromBytes : unnudgeRight(_itemFromBytes);
            appendDisplayableKeySegment(workKey, sb, from, context, true, false);
            sb.append(":");
            final byte[] to = _rightInclusive ? _itemToBytes : unnudgeLeft(_itemToBytes);
            appendDisplayableKeySegment(workKey, sb, to, context, false, true);
            if (!allInclusive) {
                sb.append(_rightInclusive ? "]" : ")");
            }
        }

        @Override
        boolean selected(final byte[] keyBytes, final int offset, final int length) {
            int compare = compare(keyBytes, offset, length, _itemFromBytes, 0, _itemFromBytes.length);

            if (compare < 0) {
                return false;
            }

            compare = compare(keyBytes, offset, length, _itemToBytes, 0, _itemToBytes.length);

            if (compare > 0) {
                return false;
            }
            return true;
        }

        @Override
        boolean atEdge(final byte[] keyBytes, final int offset, final int length, final boolean forward) {
            if (forward) {
                return compare(keyBytes, offset, length, _itemToBytes, 0, _itemToBytes.length) == 0;
            } else {
                return compare(keyBytes, offset, length, _itemFromBytes, 0, _itemFromBytes.length) == 0;
            }
        }

        @Override
        boolean forward(final Key key, final int offset, final int length) {
            final byte[] keyBytes = key.getEncodedBytes();

            final int compare = compare(keyBytes, offset, length, _itemFromBytes, 0, _itemFromBytes.length);

            if (compare < 0) {
                System.arraycopy(_itemFromBytes, 0, keyBytes, offset, _itemFromBytes.length);
                key.setEncodedSize(offset + _itemFromBytes.length);
                keyBytes[offset + _itemFromBytes.length] = 0;
                return true;
            }
            return false;
        }

        @Override
        boolean backward(final Key key, final int offset, final int length) {
            final byte[] keyBytes = key.getEncodedBytes();

            final int compare = compare(keyBytes, offset, length, _itemToBytes, 0, _itemToBytes.length);

            if (compare > 0) {
                System.arraycopy(_itemToBytes, 0, keyBytes, offset, _itemToBytes.length);
                key.setEncodedSize(offset + _itemToBytes.length);
                keyBytes[offset + _itemToBytes.length] = 0;
                return true;
            }
            return false;
        }

        @Override
        byte[] leftBytes() {
            return _itemFromBytes;
        }

        @Override
        byte[] rightBytes() {
            return _itemToBytes;
        }

        private byte[] nudgeRight(final byte[] from) {
            final int size = from.length;
            final byte[] to = new byte[size];
            System.arraycopy(from, 0, to, 0, size);
            if (size > 1 && to[size - 1] == 0) {
                to[size - 1] = 1;
            }
            return to;
        }

        private byte[] nudgeLeft(final byte[] from) {
            final int size = from.length;
            final byte[] to;
            if (size > 1 && from[size - 1] == 0 && from[size - 2] != 0) {
                to = new byte[size - 1];
                System.arraycopy(from, 0, to, 0, size - 1);
            } else {
                to = new byte[size];
                System.arraycopy(from, 0, to, 0, size);
            }
            return to;
        }

        private byte[] unnudgeRight(final byte[] from) {
            final int size = from.length;
            final byte[] to = new byte[size];
            System.arraycopy(from, 0, to, 0, size);
            if (size > 1 && to[size - 1] == 1) {
                to[size - 1] = 0;
            }
            return to;
        }

        private byte[] unnudgeLeft(final byte[] from) {
            final int size = from.length;
            final byte[] to;
            if (size > 1 && from[size - 1] != 0) {
                to = new byte[size + 1];
                System.arraycopy(from, 0, to, 0, size);
            } else {
                to = from;
            }
            return to;
        }
    }

    static class OrTerm extends Term {
        Term[] _terms;

        OrTerm(final Term[] terms) {
            _terms = new Term[terms.length];
            byte[] previousBytes = null;
            for (int index = 0; index < terms.length; index++) {
                if (terms[index] instanceof OrTerm) {
                    throw new IllegalArgumentException("Nested OrTerm at index " + index);
                }
                if (index > 0) {
                    if (compare(previousBytes, terms[index].leftBytes()) > 0) {
                        throw new IllegalArgumentException("Overlapping Term at index " + index);
                    }
                    previousBytes = terms[index].rightBytes();
                }
                _terms[index] = terms[index];
            }
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <code>Term</code>, using the supplied
         * CoderContext when necessary.
         * 
         * @param context
         *            A <code>CoderContext</code> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <code>Term</code>. May be <code>null</code>.
         * 
         * @return A canonical String representation
         */
        @Override
        public void toString(final CoderContext context, final StringBuilder sb) {
            sb.append("{");
            for (int index = 0; index < _terms.length; index++) {
                if (index > 0)
                    sb.append(',');
                _terms[index].toString(context, sb);
            }
            sb.append("}");
        }

        /**
         * Indicates whether two <code>Term</code> instances are equal. They are
         * equal if the segment values delimiting their ranges and their start-
         * and end-point inclusion settings are the same.
         * 
         * @param object
         *            The Object to be compared
         * 
         * @return <code>true</code> if the supplied object is equal to this
         *         <code>Term</code>; otherwise <code>false</code>.
         */
        @Override
        public boolean equals(final Object object) {
            if (object instanceof OrTerm) {
                final OrTerm t = (OrTerm) object;
                if (t.hashCode() != hashCode() || t._terms.length != _terms.length)
                    return false;
                for (int index = 0; index < _terms.length; index++) {
                    if (t._terms[index] != _terms[index])
                        return false;
                }
            }
            return true;
        }

        /**
         * Computes a hash code for this <code>Term</code>.
         * 
         * @return The hash code
         */
        @Override
        public int hashCode() {
            if (_hashCode == -1) {
                for (int index = 0; index < _terms.length; index++) {
                    _hashCode ^= _terms[index].hashCode();
                }
                _hashCode &= 0x7FFFFFFF;
            }
            return _hashCode;
        }

        @Override
        boolean selected(final byte[] keyBytes, final int offset, final int length) {
            for (int index = 0; index < _terms.length; index++) {
                if (_terms[index].selected(keyBytes, offset, length)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        boolean atEdge(final byte[] keyBytes, final int offset, final int length, final boolean forward) {
            for (int index = 0; index < _terms.length; index++) {
                if (_terms[index].selected(keyBytes, offset, length)
                        && _terms[index].atEdge(keyBytes, offset, length, forward)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        boolean forward(final Key key, final int offset, final int length) {
            for (int index = 0; index < _terms.length; index++) {
                if (_terms[index].forward(key, offset, length))
                    return true;
            }
            return false;
        }

        @Override
        boolean backward(final Key key, final int offset, final int length) {
            for (int index = _terms.length; --index >= 0;) {
                if (_terms[index].backward(key, offset, length))
                    return true;
            }
            return false;
        }

        @Override
        byte[] leftBytes() {
            return null;
        }

        @Override
        byte[] rightBytes() {
            return null;
        }

    }

    /**
     * Returns a <code>Term</code> that matches a single value. The value is
     * interpreted in the same manner and has the same restrictions as described
     * for the {@link Key} class.
     * 
     * @param value
     *            The value
     * 
     * @return The <code>Term</code>.
     */
    public static Term simpleTerm(final Object value) {
        return rangeTerm(value, null, true, true, null);
    }

    /**
     * Returns a <code>Term</code> that matches a single value. The value is
     * interpreted in the same manner and has the same restrictions as described
     * for the {@link Key} class.
     * 
     * @param value
     *            The value
     * 
     * @param context
     *            A <code>CoderContext</code> supplied to any registered
     *            {@link com.persistit.encoding.KeyCoder} used in encoding the
     *            <code>fromValue</code> or <code>toValue</code>. May be
     *            <code>null</code>.
     * 
     * @return The <code>Term</code>.
     */
    public static Term simpleTerm(final Object value, final CoderContext context) {
        return rangeTerm(value, null, true, true, context);
    }

    /**
     * Returns a <code>Term</code> that accepts a range of values. The range
     * includes these two values and all values that lie between them according
     * to the <a href="Key.html#_keyOrdering">key ordering specification</a>.
     * 
     * @param fromValue
     *            The first value that will be selected by this term
     * 
     * @param toValue
     *            The last value that will be selected by this term
     * 
     * @return The <code>term</code>
     * 
     * @throws IllegalArgumentException
     *             if <code>fromValue</code> follows <code>toValue</code>.
     */
    public static Term rangeTerm(final Object fromValue, final Object toValue) {
        return rangeTerm(fromValue, toValue, true, true, null);
    }

    /**
     * Returns a <code>Term</code> that accepts a range of values. The range
     * includes these two values and all values that lie between them according
     * to the <a href="Key.html#_keyOrdering">key ordering specification</a>.
     * 
     * @param fromValue
     *            The first value that will be selected by this term
     * 
     * @param toValue
     *            The last value that will be selected by this term
     * 
     * @param context
     *            A <code>CoderContext</code> supplied to any registered
     *            {@link com.persistit.encoding.KeyCoder} used in encoding the
     *            <code>fromValue</code> or <code>toValue</code>. May be
     *            <code>null</code>.
     * 
     * @return The <code>term</code>
     * 
     * @throws IllegalArgumentException
     *             if <code>fromValue</code> follows <code>toValue</code>.
     */
    public static Term rangeTerm(final Object fromValue, final Object toValue, final CoderContext context) {
        return rangeTerm(fromValue, toValue, true, true, context);
    }

    /**
     * Returns a <code>Term</code> that accepts a range of values. The range
     * optionally includes these two values and all values that lie between them
     * according to the <a href="Key.html#_keyOrdering">key ordering
     * specification</a>.
     * 
     * @param fromValue
     *            The first value that will be selected by this term
     * 
     * @param toValue
     *            The last value that will be selected by this term
     * 
     * @param leftInclusive
     *            Indicates whether a value exactly matching
     *            <code>fromValue</code> should be selected by this
     *            <code>Term</code>.
     * 
     * @param rightInclusive
     *            Indicates whether a value exactly matching
     *            <code>toValue</code> should be selected by this
     *            <code>Term</code>.
     * 
     * @return The <code>term</code>
     * 
     * @throws IllegalArgumentException
     *             if <code>fromValue</code> follows <code>toValue</code>.
     */

    public static Term rangeTerm(final Object fromValue, final Object toValue, final boolean leftInclusive,
            final boolean rightInclusive) {
        return rangeTerm(fromValue, toValue, leftInclusive, rightInclusive, null);
    }

    /**
     * Returns a <code>Term</code> that accepts a range of values. The range
     * optionally includes these two values and all values that lie between them
     * according to the <a href="Key.html#_keyOrdering">key ordering
     * specification</a>.
     * 
     * @param fromValue
     *            The first value that will be selected by this term
     * 
     * @param toValue
     *            The last value that will be selected by this term
     * 
     * @param leftInclusive
     *            Indicates whether a value exactly matching
     *            <code>fromValue</code> should be selected by this
     *            <code>Term</code>.
     * 
     * @param rightInclusive
     *            Indicates whether a value exactly matching
     *            <code>toValue</code> should be selected by this
     *            <code>Term</code>.
     * 
     * 
     * @param context
     *            A <code>CoderContext</code> supplied to any registered
     *            {@link com.persistit.encoding.KeyCoder} used in encoding the
     *            <code>fromValue</code> or <code>toValue</code>. May be
     *            <code>null</code>.
     * 
     * @return The <code>term</code>
     * 
     * @throws IllegalArgumentException
     *             if <code>fromValue</code> follows <code>toValue</code>.
     */
    public static Term rangeTerm(final Object fromValue, final Object toValue, final boolean leftInclusive,
            final boolean rightInclusive, final CoderContext context) {
        final Key key = new Key((Persistit) null);
        if (fromValue == null)
            key.appendBefore();
        else
            key.append(fromValue, context);
        key.reset();
        final byte[] leftBytes = segmentBytes(key);
        key.clear();
        if (toValue != fromValue && toValue != null) {
            key.append(toValue, context);
            key.reset();
            final byte[] rightBytes = segmentBytes(key);
            if (compare(leftBytes, rightBytes) > 0) {
                throw new IllegalArgumentException("Start value \"" + fromValue + "\" is after end value \"" + toValue
                        + "\".");
            }
            return new RangeTerm(leftBytes, rightBytes, leftInclusive, rightInclusive);
        } else
            return new SimpleTerm(leftBytes);
    }

    /**
     * Returns a <code>Term</code> that accepts a range of values. The range is
     * specified by values already encoded in two supplied {@link Key}s. The
     * index of each Key object should be set on entry to the segment to be used
     * in constructing the RangeTerm. As a side-effect, the index of each key is
     * advanced to the next segment. If the two key segments are identical and
     * if both leftInclusive and rightInclusive are true, this method returns a
     * SimpleTerm containing the segment.
     * 
     * @param fromKey
     *            A <code>Key</tt? from which the low value in the range is
     *            extracted
     * 
     * @param toKey
     *            A <code>Key</tt? from which the high value in the range is
     *            extracted
     * 
     * @param leftInclusive
     *            Indicates whether a value exactly matching <code>fromValue
     *            </code> should be selected by this <code>Term</code>.
     * 
     * @param rightInclusive
     *            Indicates whether a value exactly matching
     *            <code>toValue</code> should be selected by this
     *            <code>Term</code>.
     * 
     * @return The <code>term</code>
     */
    public static Term termFromKeySegments(final Key fromKey, final Key toKey, final boolean leftInclusive,
            final boolean rightInclusive) {
        final byte[] leftBytes = segmentBytes(fromKey);
        final byte[] rightBytes = segmentBytes(toKey);
        toKey.nextElementIndex();
        if (leftInclusive && rightInclusive && compare(leftBytes, rightBytes) == 0) {
            return new SimpleTerm(leftBytes);
        } else {
            return new RangeTerm(leftBytes, rightBytes, leftInclusive, rightInclusive);
        }
    }

    /**
     * Returns a <code>Term</code> that selects a key segment value if and only
     * if one of the members of the supplied <code>terms</code> array selects
     * it. The <code>terms</code> array may not include a nested
     * <code>OrTerm</code>.
     * 
     * @param terms
     *            Array of <code>RangeTerm</code>s or <code>SimpleTerm</code>s.
     * 
     * @return The <code>term</code>
     * 
     * @throws IllegalArgumentException
     *             if any member of the <code>terms</code> array is itself an
     *             <code>OrTerm</code> or if the end points of the terms in that
     *             array are not strictly increasing in <a
     *             href="Key.html#_keyOrdering">key order</a>.
     */
    public static Term orTerm(final Term[] terms) {
        return new OrTerm(terms);
    }

    static byte[] segmentBytes(final Key key) {
        final int from = key.getIndex();
        int to = key.nextElementIndex();
        if (to < 0)
            to = key.getEncodedSize();
        if (to <= 0)
            throw new MissingKeySegmentException();
        final byte[] bytes = new byte[to - from];
        System.arraycopy(key.getEncodedBytes(), from, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Returns the current size of this <code>KeyFilter</code>'s term array.
     * 
     * @return The size.
     */
    public int size() {
        return _terms.length;
    }

    /**
     * Returns the minimum {@link Key#getDepth() depth} of a key value that will
     * be selected by this <code>KeyFilter</code>.
     * 
     * @return The minimum depth
     */
    public int getMinimumDepth() {
        return _minDepth;
    }

    /**
     * Returns the maximum {@link Key#getDepth() depth} of a key value that will
     * be selected by this <code>KeyFilter</code>.
     * 
     * @return The maximum depth
     */
    public int getMaximumDepth() {
        return _maxDepth;
    }

    /**
     * Returns the term at the specified index.
     * 
     * @param index
     *            The index of the term to be returned.
     * 
     * @return The <code>Term</code>.
     * 
     * @throws ArrayIndexOutOfBoundsException
     *             if <code>index</code> is less than zero or greater than or
     *             equal to the number of terms in the term array.
     */
    public Term getTerm(final int index) {
        return _terms[index];
    }

    /**
     * Returns a <a href="#_stringRepresentation">string representation</a> of
     * this <code>KeyFilter</code>
     * 
     * @return The canonical string representation
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        int size = _terms.length;
        if (_maxDepth > size && _maxDepth < 100)
            size = _maxDepth;

        for (int index = 0; index < size; index++) {
            if (index > 0) {
                sb.append(",");
                if (index + 1 == _minDepth)
                    sb.append(">");
            }
            if (index >= _terms.length)
                sb.append(ALL);
            else
                _terms[index].toString(null, sb);
            if (index + 1 == _maxDepth)
                sb.append("<");
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Indicates whether the supplied key is selected by this filter.
     * 
     * @param key
     *            The <code>Key</code> value to test.
     * 
     * @return <code>true</code> if the supplied <code>Key</code> state
     *         satisfies the constraints of this filter, otherwise
     *         <code>false</code>.
     */
    public boolean selected(final Key key) {

        int index = 0;
        final int size = key.getEncodedSize();
        final byte[] keyBytes = key.getEncodedBytes();

        for (int level = 0;; level++) {
            if (index == size) {
                return (level >= _minDepth);
            } else if (level >= _maxDepth) {
                return false;
            } else {
                int nextIndex = key.nextElementIndex(index);
                if (nextIndex == -1) {
                    nextIndex = key.getEncodedSize();
                }
                final Term term = level < _terms.length ? _terms[level] : ALL;
                if (term == null || !term.selected(keyBytes, index, nextIndex - index)) {
                    return false;
                }
                index = nextIndex;
            }
        }
    }

    /**
     * <p>
     * Determine the next key value from which B-Tree traversal should proceed.
     * </p>
     * <p>
     * A KeyFilter defines a subset of the the set of all Key values: the
     * {@link Exchange#traverse(Key.Direction, KeyFilter, int)} method returns
     * only values in this subset. The following definitions are used in
     * describing the behavior of this method.
     * </p>
     * <p>
     * <dl>
     * <dt>Range</dt>
     * <dd>Let S be the ordered set of all possible key values. (Though large,
     * this set is finite because of maximum length of a key.) T range R is the
     * subset of S selected by this KeyFilter.</dd>
     * <dt>Adjacent</dt>
     * <dd>Two keys K1 and K2 in this set are <i>adjacent</i> if K1 != K2 and
     * there exists no other key value K such that K1 &lt; K &lt; k2.</dd>
     * <dt>Contiguous</dt>
     * <dd>Let C be a subset of R. Let Kmin and Kmax be the smallest and largest
     * keys in C, respectively. Then C is <i>contiguous</i> if there is no key K
     * where K1 &lt; K &lt; K2 and K is not in R.</dd>
     * </dl>
     * 
     * A KeyFilter defines a subset of S
     * </p>
     * <p>
     * This method modifies the supplied key as needed so that only key values
     * in the range are traversed. For example suppose the KeyFilter admits key
     * values between {5} and {10} (inclusive) and suppose the key currently
     * contains {3}. Then if the traversal direction is GTEQ, this method
     * modifies key to {5}, which is the next smallest memory of the range. If
     * the direction is GT, this method modifies the value of key to {5}-, which
     * is a pseudo-value immediately before {5}.
     * </p>
     * <p>
     * In most cases, if the supplied key is <i>selected</i> (in the range) then
     * this method returns <code>true</code> and does not modify the key. The
     * exception is that if the current key is selected, the direction is LT or
     * GT, and there is no adjacent key in the range, this method w
     * <p>
     * </p>
     * Similarly, if key is {12} and then the directions LTEQ and LT result in
     * key values {10} and {10}+, respectively. </p>
     * <p>
     * The return value indicates whether there exist any remaining values in
     * the range. For example, if the value of key is {10} and the direction is
     * GT, then this method returns <code>false</code>.
     * </p>
     * 
     * @param key
     *            The <code>Key</code>
     * 
     * @param direction
     *            Direction specified in the <code>traverse</code> method using
     *            this KeyFilter
     * 
     * @return <code>true</code> if a successor (or predecessor) key exists,
     *         otherwise <code>false</code>.
     */
    public boolean next(final Key key, final Key.Direction direction) {
        return next(key, 0, 0, direction == Key.GT || direction == Key.GTEQ, direction == Key.GTEQ
                || direction == Key.LTEQ);
    }

    /**
     * Process the a Term in the KeyFilter. The first <code>level</code> terms
     * of the KeyFilter have already been satisfied.
     * 
     * @param key
     * @param index
     * @param level
     * @param forward
     * @param eq
     * @return whether there may be more matching keys
     */
    private boolean next(final Key key, final int index, final int level, final boolean forward, final boolean eq) {

        int size = key.getEncodedSize();
        final byte[] bytes = key.getEncodedBytes();
        final Term term = level >= _terms.length ? ALL : _terms[level];
        int nextIndex;

        Debug.$assert0.t(level < _maxDepth);
        Debug.$assert0.t(index <= size);

        if (term == null) {
            return false;
        }

        /*
         * If at end of key and the key is deep enough to satisfy the KeyFilter,
         * then
         */
        if (forward && size == index && level >= _minDepth) {
            return true;
        }

        if (size == index) {
            nextIndex = size;
        } else {
            nextIndex = key.nextElementIndex(index);
            if (nextIndex == -1) {
                nextIndex = size;
            }
        }

        boolean isLastKeySegment = nextIndex == size;

        for (;;) {
            if (term.selected(bytes, index, nextIndex - index)) {
                if (level + 1 == _maxDepth) {
                    if (isLastKeySegment) {
                        if (eq || !term.atEdge(bytes, index, nextIndex - index, forward)) {
                            return true;
                        }
                    } else {
                        //
                        // The Key is deeper than this KeyFilter's max depth.
                        // Therefore truncate the key, which results in a
                        // smaller key value than the original. If the traversal
                        // direction is LT or LTEQ, then truncating the key is
                        // all that's needed nudge the key leftward to avoid
                        // traversing the same subtree. If the direction is
                        // GT or GTEQ, then the key needs to be nudged past
                        // any children.
                        //
                        key.setEncodedSize(nextIndex);
                        isLastKeySegment = true;

                        if (key.isSpecial()) {
                            return true;
                        }

                        if (forward) {
                            key.nudgeRight();
                            continue;
                        } else {
                            if (!eq) {
                                key.nudgeDeeper();
                            }
                            return true;
                        }
                    }
                } else if (level + 1 < _minDepth) {
                    if (key.isSegmentSpecial(nextIndex) || (forward || !isLastKeySegment)
                            && next(key, nextIndex, level + 1, forward, eq)) {
                        return true;
                    }
                } else if (isLastKeySegment) {
                    if (eq || (forward && level + 1 < _maxDepth)
                            || !term.atEdge(bytes, index, nextIndex - index, forward)) {
                        return true;
                    }
                } else {
                    if (key.isSegmentSpecial(nextIndex) || next(key, nextIndex, level + 1, forward, eq)) {
                        return true;
                    }
                }
            }

            //
            // If the term was not selected, or if a deeper level of the
            // KeyFilter was exhausted, then attempt to modify the current
            // key segment to a value at the edge of a new contiguous
            // area of the range.
            //

            key.setEncodedSize(nextIndex);

            if (forward) {
                if (term.selected(key.getEncodedBytes(), index, nextIndex - index)
                        && !term.atEdge(key.getEncodedBytes(), index, nextIndex - index, forward)) {
                    key.nudgeRight();
                    return true;
                } else {
                    if (!term.forward(key, index, nextIndex - index)) {
                        return false;
                    }
                }
            } else {
                if (term.selected(key.getEncodedBytes(), index, nextIndex - index)
                        && !term.atEdge(key.getEncodedBytes(), index, nextIndex - index, forward)) {
                    key.nudgeLeft();
                    return true;
                } else {
                    if (!term.backward(key, index, nextIndex - index)) {
                        return false;
                    }
                }
            }
            size = key.getEncodedSize();
            nextIndex = size;
            isLastKeySegment = true;

            if (key.isSpecial()) {
                return true;
            }

            if (forward) {
                if (level + 1 >= _minDepth) {
                    if (!eq) {
                        // For the GT case, choose the left-adjacent key.
                        key.nudgeLeft();
                    }
                    return true;
                }
            } else {
                if (level + 1 < _maxDepth) {
                    key.appendAfter();
                    return next(key, nextIndex, level + 1, forward, eq);
                } else {
                    if (!eq) {
                        key.nudgeRight();
                    }
                    return true;
                }
            }
        }
    }

    private static int compare(final byte[] a, final byte[] b) {
        if ((a == b) || (a == null && b == null))
            return 0;
        if (a == null && b != null)
            return Integer.MIN_VALUE;
        if (a != null && b == null)
            return Integer.MAX_VALUE;
        return compare(a, 0, a.length, b, 0, b.length);
    }

    private static int compare(final byte[] a, final int offsetA, final int sizeA, final byte[] b, final int offsetB,
            final int sizeB) {
        final int size = Math.min(sizeA, sizeB);
        for (int i = 0; i < size; i++) {
            if (a[i + offsetA] != b[i + offsetB]) {
                if ((a[i + offsetA] & 0xFF) > (b[i + offsetB] & 0xFF))
                    return 1;
                else
                    return -1;
            }
        }
        if (sizeA < sizeB)
            return -1;
        if (sizeA > sizeB)
            return 1;
        return 0;
    }

    private static int byteHash(final byte[] a) {
        if (a == null)
            return 0;
        int h = 0;
        for (int i = 0; i < a.length; i++) {
            h = h * 17 + a[i];
        }
        return h;
    }

    private static void appendDisplayableKeySegment(final Key workKey, final StringBuilder sb, final byte[] bytes,
            final CoderContext context, final boolean before, final boolean after) {
        if (bytes == null) {
            return;
        }
        System.arraycopy(bytes, 0, workKey.getEncodedBytes(), 0, bytes.length);
        workKey.setEncodedSize(bytes.length);
        if (before && workKey.isBefore() || after && workKey.isAfter()) {
            return;
        }

        try {
            workKey.decodeDisplayable(true, sb, context);
        } catch (final Exception e) {
            sb.append(e);
            sb.append("(");
            sb.append(Util.hexDump(bytes, 0, bytes.length));
            sb.append(")");
        }
    }

    boolean isKeyPrefixFilter() {
        return _isKeyPrefixFilter;
    }

    int getKeyPrefixByteCount() {
        return _keyPrefixByteCount;
    }
}
