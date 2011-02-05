/**
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Aug 13, 2004
 */
package com.persistit;

import com.persistit.encoding.CoderContext;
import com.persistit.exception.MissingKeySegmentException;

/**
 * <p>
 * Specifies a subset of all possible keys values. A <tt>KeyFilter</tt> can be
 * used with the {@link Exchange#traverse(Key.Direction, KeyFilter, int)} method
 * to restrict the set of key values within a Persistit <tt>Tree</tt> that will
 * actually be traversed.
 * </p>
 * <p>
 * A <tt>KeyFilter</tt> provides two primary methods:
 * <ul>
 * <li>
 * {@link #selected(Key)} indicates whether the value of the specified
 * <tt>Key</tt> is a member of the subset specified by this filter, and</li>
 * <li>
 * {@link #traverse(Key, boolean)} modifies the <tt>Key</tt> to the next larger
 * or smaller key value that lies within the subset specified by this filter.</li>
 * </ul>
 * These methods permit efficient traversal of a filtered subset of keys within
 * a <tt>Tree</tt>.
 * </p>
 * <p>
 * <h3>KeyFilter Terms</h3>
 * A <tt>KeyFilter</tt> consists of an array of one or more <i>terms</i>. Each
 * term corresponds to one segment in a key value that will be selected or
 * excluded by this <tt>KeyFilter</tt>. The <i>K</i>th term in the list applies
 * to the <i>K</i>th segment of a key.
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
 * {@link #orTerm} produce these various kinds of <tt>Term</tt>s
 * <p>
 * For example, consider a key consisting of three segments: a last name, a
 * first name and a person ID number for a person. Such a key value might be
 * constructed with code such as this:
 * 
 * <pre>
 * <blockquote><code>
 * key.clear().append("McDonald").append("Bob").append(12345);
 * </code></blockquote>
 * </pre>
 * 
 * Suppose we now want to enumerate all members of a tree having keys of this
 * form with last names falling between "M" and "Q", and first names equal to
 * either "Alice" or "Bob". The following code constructs a <tt>KeyFilter</tt>
 * for this purpose:
 * 
 * <pre>
 * <blockquote><code>
 * KeyFilter keyFilter = new KeyFilter();
 * keyFilter = keyFilter.append(KeyFilter.rangeTerm("M", "Q"));
 * keyFilter = keyFilter.append(KeyFilter.orTerm(new KeyFilter.Term[]{
 *    KeyFilter.simpleTerm("Alice"), KeyFilter.simpleTerm("Bob"))});
 * </code></blockquote>
 * </pre>
 * 
 * The first term specifies a range that includes any last name that sorts
 * alphabetically between "M" and "Q" (inclusively). The second term is an
 * OrTerm that selects the first names "Alice" and "Bob".
 * </p>
 * <p>
 * A RangeTerm optionally specifies whether the end-points are inclusive. For
 * example, the term
 * 
 * <pre>
 * <blockquote><code>
 *   KeyFilter.rangeTerm("Jones", "Smith", true, false)
 * </code></blockquote>
 * </pre>
 * 
 * includes the name "Jones" and all names that follow up to, but not including,
 * "Smith". If unspecified, the end-points of the range are included.
 * </p>
 * <p>
 * <h3>Minimum and Maximum Depth</h3>
 * A <tt>KeyFilter</tt> may also specify <i>minimum depth</i>, <i>maximum
 * depth</i>, or both. These values control the number of segments that must be
 * present in key value in order for it to be selected. A <tt>KeyFilter</tt>
 * will select a key only if the number of segments in the key lies between the
 * minimum depth and the maximum depth, inclusive.
 * </p>
 * <p>
 * <a name="_stringRepresentation" />
 * <h3>String Representation</h3>
 * The {@link #toString()} method returns a canonical String representation of
 * the current terms for this <tt>KeyFilter</tt>. For example, the string
 * representation of the filter constructed in above is
 * 
 * <pre>
 * <blockquote><code>
 * {"M":"Q",{"Alice","Bob"}}
 * </code></blockquote>
 * </pre>
 * 
 * You can construct a <tt>KeyFilter</tt> from its string representation with
 * the {@link KeyParser#parseKeyFilter} method. For example, the following code
 * generates an equivalent <tt>KeyFilter</tt>:
 * 
 * <pre>
 * <blockquote><code>
 * KeyParser parser = new KeyParser("{\"M\":\"Q\",{\"Alice\",\"Bob\"}};
 * KeyFilter filter = parser.parseKeyFilter();
 * </code></blockquote>
 * </pre>
 * 
 * As a convenience, the constructor {@link #KeyFilter(String)} automatically
 * creates and invokes a <tt>KeyParser</tt> to create a <tt>KeyFilter</tt> from
 * its string representation.
 * </p>
 * <p>
 * Following is an informal grammar for the string representation of a key
 * filter. See <a href="Key.html#_stringRepresentation">string
 * representation</a> in {@link Key} for information on how to specify a key
 * segment value.
 * 
 * <pre>
 * <blockquote><code>
 *    keyFilter ::= '{' termElement [',' termElement]... '}'
 *    termElement ::= [ '&gt;' ] term [ '&lt;' ]
 *    term ::= segment | range | qualifiedRange | orTerm
 *    segment ::= see <a href="Key.html#_stringRepresentation">segment</a>
 *    range ::= segment ':' segment | ':' segment | segment ':'
 *    qualifiedRange = ('(' | '[') range (')' | ']')
 *    orTerm ::= '{' term [',' term ]...'}'
 * </code></blockquote>
 * </pre>
 * 
 * A <i>range</i> may omit either the starting segment value or the ending
 * segment value. When the starting segment value is omitted, the range starts
 * before the first possible key value, and when the ending segment value is
 * omitted, the range ends after the last possible key value. Thus the range
 * specification
 * 
 * <pre>
 * <blockquote><code>
 *   {"Smith":}
 * </code></blockquote>
 * </pre>
 * 
 * include every key with a first segment value of "Smith" or above. Similarly,
 * 
 * <pre>
 * <blockquote><code>
 *   {:"Smith"}
 * </code></blockquote>
 * </pre>
 * 
 * includes all keys up to and including "Smith".
 * </p>
 * <p>
 * A <i>qualifiedRange</i> allows you to specify whether the end-points of a
 * range are included or excluded from the selected subset. A square bracket
 * indicates that the end-point is included, while a parenthesis indicates that
 * it is excluded. For example
 * 
 * <pre>
 * <blockquote><code>
 *   {("Jones":"Smith"]}
 * </code></blockquote>
 * </pre>
 * 
 * does not include "Jones" but does include "Smith". An unqualified
 * <i>range</i> specification such as
 * 
 * <pre>
 * <blockquote><code>
 *   {"Jones":"Smith"}
 * </code></blockquote>
 * </pre>
 * 
 * includes both end-points. It is equivelent to
 * 
 * <pre>
 * <blockquote><code>
 *   {["Jones":"Smith"]}
 * </code></blockquote>
 * </pre>
 * 
 * </p>
 * <p>
 * Within the string representation of a <tt>KeyFilter</tt> at most one term
 * element may specify the prefix "&gt;" (greater-than sign), and at most one
 * term element may specify the suffix "&lt;" (less-than sign). These denote the
 * minimum and maximum depths of the <tt>KeyFilter</tt>, respectively. The
 * minimum depth is the count of term elements up to and including the term
 * marked with a "&gt;" and the maximum depth is the count of terms up to and
 * including the term marked with a "&gt;". For example, in the
 * <tt>KeyFilter</tt> represented by the string
 * 
 * <pre>
 * <blockquote><code>
 *   {*,>100:200,*<}
 * </code></blockquote>
 * </pre>
 * 
 * the minimum depth is 2 and the maximum depth is 3.
 * </p>
 * <p>
 * <h3>Building KeyFilters by Appending Terms</h3>
 * A <tt>KeyFilter</tt> is immutable. The methods {@link #append(KeyFilter)},
 * {@link #append(KeyFilter.Term)}, {@link #append(KeyFilter.Term[])}, create
 * new <tt>KeyFilter</tt>s with additional terms supplied by the supplied
 * <tt>KeyFilter</tt>, <tt>Term</tt> or array of <tt>Term</tt>s. The
 * {@link #limit} method creates a new <tt>KeyFilter</tt> with modified minimum
 * and maximum depth values. Each of these methods returns a new
 * <tt>KeyFilter</tt> which results from combining the original
 * <tt>KeyFilter</tt> with the supplied information.
 * </p>
 * 
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
     * Constructs an empty <tt>KeyFilter</tt>. This <tt>KeyFilter</tt>
     * implicitly selects all key values.
     */
    public KeyFilter() {
        _terms = new Term[0];
    }

    /**
     * Constructs a <tt>KeyFilter</tt> from its <a
     * href="#_stringRepresentation"> string representation</a>.
     * 
     * @param string
     *            The string representation
     * 
     * @throws IllegalArgumentException
     *             if the string is not valid
     */
    public KeyFilter(String string) {
        KeyParser parser = new KeyParser(string);
        KeyFilter filter = parser.parseKeyFilter();
        if (filter == null) {
            throw new IllegalArgumentException("Invalid KeyFilter expression");
        } else {
            _terms = filter._terms;
            _minDepth = filter._minDepth;
            _maxDepth = filter._maxDepth;
        }
    }

    /**
     * Constructs a <tt>KeyFilter</tt> that selects the subset of all keys which
     * are equal to, <a href="Key.html#_keyChildren">logical children</a> of, or
     * logical ancestors of the supplied <tt>Key</tt>.
     * 
     * @param key
     *            The <tt>Key</tt>
     */
    public KeyFilter(Key key) {
        this(key, 0, Integer.MAX_VALUE);
    }

    /**
     * Constructs a <tt>KeyFilter</tt> that selects the subset of all key values
     * that are equal to, <a href="Key.html#_keyChildren">logical children</a>
     * of, or logical ancestors of the supplied <tt>Key</tt>, and whose depth is
     * greater than or equal to the supplied minimum depth and less than or
     * equal to the supplied maximum depth. Suppose the supplied <tt>key</tt>
     * value has <i>M</i> segments and some other <tt>Key</tt> value <i>K</i>
     * has <i>N</i> segments. Then <i>K</i> is a member of the subset selected
     * by this <tt>KeyFilter</tt> if and only if <i>N</i>&gt;=<tt>minDepth</tt>
     * and <i>N</i>&lt;=<tt>maxDepth</tt> and each of the first min(<i>M</i>,
     * <i>N</i>) segments match.
     * 
     * @param key
     *            The <tt>Key</tt>
     * 
     * @param minDepth
     *            The minimum depth
     * 
     * @param maxDepth
     *            The maximum depth
     * 
     */
    public KeyFilter(Key key, int minDepth, int maxDepth) {
        checkLimits(minDepth, maxDepth);
        int depth = key.getDepth();
        _terms = new Term[depth];
        int size = key.getEncodedSize();
        int index = 0;
        if (key != null && size != 0) {
            for (int level = 0;; level++) {
                int previous = index;
                index = key.nextElementIndex(previous);
                if (index < 0)
                    break;
                byte[] bytes = new byte[index - previous];
                System.arraycopy(key.getEncodedBytes(), previous, bytes, 0,
                        bytes.length);
                _terms[level] = new SimpleTerm(bytes);
            }
        }
        _minDepth = minDepth;
        _maxDepth = maxDepth;
    }

    /**
     * Constructs a <tt>KeyFilter</tt> that selects the subset of all keys whose
     * segments are selected by the corresponding <tt>Term</tt>s of the supplied
     * array. Suppose a Key <i>K</i> has <i>N</i> segments and the supplied
     * array of <tt>terms</tt> has length <i>M</i>. Then <i>K</i> is a member of
     * the key value subset selected by this <tt>KeyFilter</tt> if and only if
     * each of the first min(<i>M</i>, <i>N</i>) segments of <i>K</i> is
     * selected by the corresponding member of the <tt>terms</tt> array.
     * 
     * @param terms
     */
    public KeyFilter(Term[] terms) {
        this(terms, 0, Integer.MAX_VALUE);
    }

    /**
     * Constructs a <tt>KeyFilter</tt> that selects the subset of all keys whose
     * segments are selected by the corresponding <tt>Term</tt>s of the supplied
     * array and whose depth is greater than or equal to the supplied minimum
     * depth and less than or equal to the supplied maximum depth. Suppose some
     * Key <i>K</i> has <i>N</i> segments and the supplied array of
     * <tt>terms</tt> has length <i>M</i>. Then <i>K</i> is a member of the key
     * value subset selected by this <tt>KeyFilter</tt> if and only if and only
     * if <i>N</i>&gt;=<tt>minDepth</tt> and <i>N</i>&lt;=<tt>maxDepth</tt> and
     * each of the first min(<i>M</i>, <i>N</i>) segments of <i>K</i> is
     * selected by the corresponding member of the <tt>terms</tt> array.
     * 
     * @param terms
     *            The <tt>Term</tt> array
     * 
     * @param minDepth
     *            The minimum depth
     * 
     * @param maxDepth
     *            The maximum depth
     * 
     */
    public KeyFilter(Term[] terms, int minDepth, int maxDepth) {
        checkLimits(minDepth, maxDepth);
        _terms = terms;
        _minDepth = minDepth;
        _maxDepth = maxDepth;
    }

    /**
     * Constructs and returns a new <tt>KeyFilter</tt> in which the terms of the
     * supplied <tt>filter</tt> are appended to the array of terms already
     * present in this <tt>KeyFilter</tt>. In addition, the minimum and maximum
     * depths of the newly created <tt>KeyFilter</tt> are computed from the
     * supplied <tt>filter</tt> value. Let M be the number of terms in this
     * <tt>KeyFilter</tt>. The the minimum and maximum depth parameters for the
     * newly created <tt>KeyFilter</tt> will be
     * <tt>filter.getMinimumDepth()+</tt>M and
     * <tt>filter.getMaximumDepth()+</tt>M, respectively.
     * 
     * @param filter
     *            The <tt>KeyFilter</tt> to append
     * 
     * @return The newly constructed <tt>KeyFilter</tt>.
     */
    public KeyFilter append(KeyFilter filter) {
        KeyFilter newFilter = new KeyFilter(merge(_terms, filter._terms));

        int size = _terms.length;

        newFilter._minDepth = filter._minDepth + size;

        if (Integer.MAX_VALUE - size > filter._maxDepth) {
            newFilter._maxDepth = filter._maxDepth + size;
        }

        return newFilter;
    }

    /**
     * Constructs and returns a new <tt>KeyFilter</tt> in which the supplied
     * <tt>term</tt> is appended to the end of the array of terms in the current
     * <tt>KeyFilter</tt>.
     * 
     * @param term
     *            The <tt>Term</tt> to append
     * 
     * @return The newly constructed <tt>KeyFilter</tt>
     */
    public KeyFilter append(Term term) {
        Term[] newTerms = new Term[_terms.length + 1];
        System.arraycopy(_terms, 0, newTerms, 0, _terms.length);
        newTerms[_terms.length] = term;
        return new KeyFilter(newTerms);
    }

    /**
     * Constructs and returns a new <tt>KeyFilter</tt> in which the supplied
     * <tt>terms</tt> are appended to the array of terms in the current
     * <tt>KeyFilter</tt>.
     * 
     * @param terms
     *            The array of <tt>Term</tt> to append
     * 
     * @return The newly constructed <tt>KeyFilter</tt>
     */
    public KeyFilter append(Term[] terms) {
        Term[] newTerms = merge(_terms, terms);
        return new KeyFilter(newTerms);
    }

    /**
     * Constructs and returns a new <tt>KeyFilter</tt> in which the minimum and
     * maximum depth are set to the supplied values.
     * 
     * @param minDepth
     *            The minimum depth
     * 
     * @param maxDepth
     *            The maximum depth
     * 
     * @return The newly constructed <tt>KeyFilter</tt>.
     */
    public KeyFilter limit(int minDepth, int maxDepth) {
        checkLimits(minDepth, maxDepth);
        KeyFilter newFilter = new KeyFilter(_terms);
        newFilter._minDepth = minDepth;
        newFilter._maxDepth = maxDepth;
        return newFilter;
    }

    private void checkLimits(int minDepth, int maxDepth) {
        if (minDepth < 0) {
            throw new IllegalArgumentException("minDepth (" + minDepth
                    + ") must be >= 0");
        }
        if (minDepth > maxDepth) {
            throw new IllegalArgumentException("minDepth (" + minDepth
                    + ") must be <= maxDepth (" + maxDepth + ")");
        }
    }

    private Term[] merge(Term[] a, Term[] b) {
        int sizeA = a == null ? 0 : a.length;
        int sizeB = b == null ? 0 : b.length;
        if (sizeA == 0 && b != null)
            return b;
        if (sizeB == 0 && a != null)
            return a;
        Term[] terms = new Term[sizeA + sizeB];
        if (sizeA > 0)
            System.arraycopy(a, 0, terms, 0, sizeA);
        if (sizeB > 0)
            System.arraycopy(b, 0, terms, sizeA, sizeB);
        return terms;
    }

    /**
     * <p>
     * Specifies criteria for selecting one segment of a <tt>Key</tt> value.
     * This abstract class has three concrete subclasses, <tt>SimpleTerm</tt>,
     * <tt>RangeTerm</tt> and <tt>OrTerm</tt> as described for {@link KeyFilter}
     * .
     * </p>
     * <p>
     * Use the static factory methods {@link KeyFilter#simpleTerm(Object)},
     * {@link KeyFilter#rangeTerm(Object, Object)},
     * {@link KeyFilter#rangeTerm(Object, Object, CoderContext)},
     * {@link KeyFilter#rangeTerm(Object, Object, boolean, boolean)},
     * {@link KeyFilter#rangeTerm(Object, Object, boolean, boolean, CoderContext)}
     * , and {@link KeyFilter#orTerm} to create instances of <tt>Term</tt>.
     */
    public static abstract class Term {
        protected int _hashCode = -1;

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <tt>Term</tt>.
         * 
         * @return A canonical String representation
         */
        @Override
        public String toString() {
            return toString(null);
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <tt>Term</tt> using the supplied
         * <tt>context</tt>. The <tt>context</tt> is used only if the segment
         * value for a <tt>SimpleTerm</tt>, or values for a <tt>RangeTerm</tt>
         * are members of a class with a registered
         * {@link com.persistit.encoding.KeyCoder} the uses a
         * {@link com.persistit.encoding.CoderContext}.
         * 
         * @param context
         *            A <tt>CoderContext</tt> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <tt>Term</tt>. May be <tt>null</tt>.
         * 
         * @return A canonical String representation
         */
        public String toString(CoderContext context) {
            StringBuilder sb = new StringBuilder();
            toString(context, sb);
            return sb.toString();
        }

        abstract void toString(CoderContext context, StringBuilder sb);

        abstract boolean selected(byte[] keyBytes, int offset, int length);
        
        abstract boolean exhausted(byte[] keyBytes, int offset, int length, Key.Direction direction);

        abstract boolean forward(Key key, int offset, int length);

        abstract boolean backward(Key key, int offset, int length);

        abstract byte[] leftBytes();

        abstract byte[] rightBytes();
    }

    static class SimpleTerm extends Term {
        private byte[] _itemBytes;

        SimpleTerm(byte[] bytes) {
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
        public boolean equals(Object object) {
            if (object instanceof SimpleTerm) {
                SimpleTerm t = (SimpleTerm) object;
                return compare(_itemBytes, t._itemBytes) == 0;
            }
            return false;
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <tt>Term</tt>, using the supplied
         * CoderContext when necessary.
         * 
         * @param context
         *            A <tt>CoderContext</tt> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <tt>Term</tt>. May be <tt>null</tt>.
         * 
         * @return A canonical String representation
         */
        @Override
        public void toString(CoderContext context, StringBuilder sb) {
            if (this == ALL)
                sb.append("*");
            else {
                final Key workKey = new Key((Persistit) null);
                appendDisplayableKeySegment(workKey, sb, _itemBytes, context,
                        false, false);
            }
        }

        @Override
        boolean selected(byte[] keyBytes, int offset, int length) {
            if (this == ALL)
                return true;

            if (length != _itemBytes.length)
                return false;
            for (int index = 0; index < length; index++) {
                if (_itemBytes[index] != keyBytes[offset + index])
                    return false;
            }
            return true;
        }
        
        @Override
        boolean exhausted(byte[] keyBytes, int offset, int length, Key.Direction direction) {
            if (direction == Key.LT || direction == Key.GT){
                return true;
            } else {
                return !selected(keyBytes, offset, length);
            }
        }

        @Override
        boolean forward(Key key, int offset, int length) {
            if (this == ALL) {
                key.setEncodedSize(offset + length);
                key.nudgeRight();
                return true;
            }

            byte[] keyBytes = key.getEncodedBytes();

            int compare = compare(keyBytes, offset, length, _itemBytes, 0,
                    _itemBytes.length);

            if (compare < 0) {
                System.arraycopy(_itemBytes, 0, keyBytes, offset,
                        _itemBytes.length);
                key.setEncodedSize(offset + _itemBytes.length);
                return true;
            } else
                return false;
        }

        @Override
        boolean backward(Key key, int offset, int length) {
            if (this == ALL) {
                key.setEncodedSize(offset + length);
                key.nudgeLeft();
                return true;
            }

            byte[] keyBytes = key.getEncodedBytes();

            int compare = compare(keyBytes, offset, length, _itemBytes, 0,
                    _itemBytes.length);

            if (compare > 0) {
                System.arraycopy(_itemBytes, 0, keyBytes, offset,
                        _itemBytes.length);
                key.setEncodedSize(offset + _itemBytes.length);
                return true;
            } else
                return false;
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
     * appropriate <tt>Term</tt> instances.
     */
    static class RangeTerm extends Term {
        private boolean _leftInclusive = true;
        private boolean _rightInclusive = true;
        private byte[] _itemFromBytes;
        private byte[] _itemToBytes;

        RangeTerm(byte[] leftBytes, byte[] rightBytes, boolean leftInclusive,
                boolean rightInclusive) {
            _leftInclusive = leftInclusive;
            _rightInclusive = rightInclusive;
            _itemFromBytes = leftBytes;
            _itemToBytes = rightBytes;
        }

        /**
         * Indicates whether two <tt>Term</tt> instances are equal. They are
         * equal if the segment values delimiting their ranges and their start-
         * and end-point inclusion settings are the same.
         * 
         * @param object
         *            The Object to be compared
         * 
         * @return <tt>true</tt> if the supplied object is equal to this
         *         <tt>Term</tt>; otherwise <tt>false</tt>.
         */
        @Override
        public boolean equals(Object object) {
            if (object instanceof RangeTerm) {
                RangeTerm t = (RangeTerm) object;
                boolean result = _leftInclusive == t._leftInclusive
                        && _rightInclusive == t._rightInclusive
                        && compare(_itemFromBytes, t._itemFromBytes) == 0
                        && compare(_itemToBytes, t._itemToBytes) == 0;
                return result;
            }
            return false;
        }

        /**
         * Computes a hash code for this <tt>Term</tt>.
         * 
         * @return The hash code
         */
        @Override
        public int hashCode() {
            if (_hashCode == -1) {
                _hashCode = (byteHash(_itemFromBytes) ^ byteHash(_itemToBytes)
                        ^ (_leftInclusive ? 123 : 0) ^ (_rightInclusive ? 456
                        : 0)) & 0x7FFFFFFF;
            }
            return _hashCode;
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <tt>Term</tt>, using the supplied
         * CoderContext when necessary.
         * 
         * @param context
         *            A <tt>CoderContext</tt> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <tt>Term</tt>. May be <tt>null</tt>.
         * 
         * @return A canonical String representation
         */
        @Override
        public void toString(CoderContext context, StringBuilder sb) {
            Key workKey = new Key((Persistit) null);
            boolean allInclusive = _leftInclusive && _rightInclusive;
            if (!allInclusive)
                sb.append(_leftInclusive ? "[" : "(");
            appendDisplayableKeySegment(workKey, sb, _itemFromBytes, context,
                    true, false);
            sb.append(":");
            appendDisplayableKeySegment(workKey, sb, _itemToBytes, context,
                    false, true);
            if (!allInclusive)
                sb.append(_rightInclusive ? "]" : ")");
        }

        @Override
        boolean selected(byte[] keyBytes, int offset, int length) {
            int compare = compare(keyBytes, offset, length, _itemFromBytes, 0,
                    _itemFromBytes.length);

            if (compare < 0 || (!_leftInclusive && compare == 0)) {
                return false;
            }

            compare = compare(keyBytes, offset, length, _itemToBytes, 0,
                    _itemToBytes.length);

            if (compare > 0 || (!_rightInclusive && compare == 0)) {
                return false;
            }
            return true;
        }
        
        @Override
        boolean exhausted(byte[] keyBytes, int offset, int length, Key.Direction direction) {
            if (direction == Key.EQ) {
                return selected(keyBytes, offset, length);
            }
            int compare = compare(keyBytes, offset, length, _itemFromBytes, 0,
                    _itemFromBytes.length);

            if (compare < 0) {
                return false;
            }
            if (compare == 0 && (direction == Key.LT || direction == Key.LTEQ && !_leftInclusive)) {
                return false;
            }

            compare = compare(keyBytes, offset, length, _itemToBytes, 0,
                    _itemToBytes.length);

            if (compare > 0) {
                return false;
            }
            
            if (compare == 0 && (direction == Key.GT || direction == Key.GTEQ && !_rightInclusive)) {
                return false;
            }
            
            return true;
        }



        @Override
        boolean forward(Key key, int offset, int length) {
            byte[] keyBytes = key.getEncodedBytes();

            boolean moved = false;

            int compare = compare(keyBytes, offset, length, _itemFromBytes, 0,
                    _itemFromBytes.length);

            if (compare < 0) {
                System.arraycopy(_itemFromBytes, 0, keyBytes, offset,
                        _itemFromBytes.length);
                key.setEncodedSize(offset + _itemFromBytes.length);
                length = _itemFromBytes.length;
                moved = true;
                if (_leftInclusive) {
                    key.nudgeLeft();
                }
            }

            compare = compare(keyBytes, offset, length, _itemToBytes, 0,
                    _itemToBytes.length);

            if (compare > 0 || (compare == 0 && (!moved || !_rightInclusive))) {
                return false;
            }

            if (!moved) {
                key.setEncodedSize(offset + length);
                key.nudgeRight();
                compare = compare(keyBytes, offset, length, _itemToBytes, 0,
                        _itemToBytes.length);
            }
            return compare < 0 || compare == 0 && _rightInclusive;
        }

        @Override
        boolean backward(Key key, int offset, int length) {
            byte[] keyBytes = key.getEncodedBytes();

            boolean moved = false;

            int compare = compare(keyBytes, offset, length, _itemToBytes, 0,
                    _itemToBytes.length);

            if (compare > 0) {
                System.arraycopy(_itemToBytes, 0, keyBytes, offset,
                        _itemToBytes.length);
                key.setEncodedSize(offset + _itemToBytes.length);
                length = _itemToBytes.length;
                moved = true;
                if (_rightInclusive) {
                    key.nudgeDeeper();
                }
            }

            compare = compare(keyBytes, offset, length, _itemFromBytes, 0,
                    _itemFromBytes.length);

            if (compare < 0 || (compare == 0 && (!moved || !_leftInclusive))) {
                return false;
            }

            if (!moved) {
                key.setEncodedSize(offset + length);
                key.nudgeLeft();
                compare = compare(keyBytes, offset, length, _itemFromBytes, 0,
                        _itemFromBytes.length);
            }
            return compare > 0 || compare == 0 && _leftInclusive;
        }

        @Override
        byte[] leftBytes() {
            return _itemFromBytes;
        }

        @Override
        byte[] rightBytes() {
            return _itemToBytes;
        }
    }

    static class OrTerm extends Term {
        Term[] _terms;

        OrTerm(Term[] terms) {
            _terms = new Term[terms.length];
            byte[] previousBytes = null;
            for (int index = 0; index < terms.length; index++) {
                if (terms[index] instanceof OrTerm) {
                    throw new IllegalArgumentException(
                            "Nested OrTerm at index " + index);
                }
                if (index > 0) {
                    if (compare(previousBytes, terms[index].leftBytes()) > 0) {
                        throw new IllegalArgumentException(
                                "Overlapping Term at index " + index);
                    }
                    previousBytes = terms[index].rightBytes();
                }
                _terms[index] = terms[index];
            }
        }

        /**
         * Returns a <a href="KeyFilter.html#_stringRepresentation"> string
         * representation</a> of this <tt>Term</tt>, using the supplied
         * CoderContext when necessary.
         * 
         * @param context
         *            A <tt>CoderContext</tt> that will be passed to any
         *            registered {@link com.persistit.encoding.KeyCoder} used in
         *            decoding the value or values representing end-points in
         *            this <tt>Term</tt>. May be <tt>null</tt>.
         * 
         * @return A canonical String representation
         */
        @Override
        public void toString(CoderContext context, StringBuilder sb) {
            sb.append("{");
            for (int index = 0; index < _terms.length; index++) {
                if (index > 0)
                    sb.append(',');
                _terms[index].toString(context, sb);
            }
            sb.append("}");
        }

        /**
         * Indicates whether two <tt>Term</tt> instances are equal. They are
         * equal if the segment values delimiting their ranges and their start-
         * and end-point inclusion settings are the same.
         * 
         * @param object
         *            The Object to be compared
         * 
         * @return <tt>true</tt> if the supplied object is equal to this
         *         <tt>Term</tt>; otherwise <tt>false</tt>.
         */
        @Override
        public boolean equals(Object object) {
            if (object instanceof OrTerm) {
                OrTerm t = (OrTerm) object;
                if (t.hashCode() != hashCode()
                        || t._terms.length != _terms.length)
                    return false;
                for (int index = 0; index < _terms.length; index++) {
                    if (t._terms[index] != _terms[index])
                        return false;
                }
            }
            return true;
        }

        /**
         * Computes a hash code for this <tt>Term</tt>.
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
        boolean selected(byte[] keyBytes, int offset, int length) {
            for (int index = 0; index < _terms.length; index++) {
                if (_terms[index].selected(keyBytes, offset, length)) {
                    return true;
                }
            }
            return false;
        }
        
        @Override
        boolean exhausted(byte[] keyBytes, int offset, int length, Key.Direction direction) {
            for (int index = 0; index < _terms.length; index++) {
                if (!_terms[index].exhausted(keyBytes, offset, length, direction)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        boolean forward(Key key, int offset, int length) {
            for (int index = 0; index < _terms.length; index++) {
                if (_terms[index].forward(key, offset, length))
                    return true;
            }
            return false;
        }

        @Override
        boolean backward(Key key, int offset, int length) {
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
     * Returns a <tt>Term</tt> that matches a single value. The value is
     * interpreted in the same manner and has the same restrictions as described
     * for the {@link Key} class.
     * 
     * @param value
     *            The value
     * 
     * @return The <tt>Term</tt>.
     */
    public static Term simpleTerm(Object value) {
        return rangeTerm(value, null, true, true, null);
    }

    /**
     * Returns a <tt>Term</tt> that matches a single value. The value is
     * interpreted in the same manner and has the same restrictions as described
     * for the {@link Key} class.
     * 
     * @param value
     *            The value
     * 
     * @param context
     *            A <tt>CoderContext</tt> supplied to any registered
     *            {@link com.persistit.encoding.KeyCoder} used in encoding the
     *            <tt>fromValue</tt> or <tt>toValue</tt>. May be <tt>null</tt>.
     * 
     * @return The <tt>Term</tt>.
     */
    public static Term simpleTerm(Object value, CoderContext context) {
        return rangeTerm(value, null, true, true, context);
    }

    /**
     * Returns a <tt>Term</tt> that accepts a range of values. The range
     * includes these two values and all values that lie between them according
     * to the <a href="Key.html#_keyOrdering">key ordering specification</a>.
     * 
     * @param fromValue
     *            The first value that will be selected by this term
     * 
     * @param toValue
     *            The last value that will be selected by this term
     * 
     * @return The <tt>term</tt>
     * 
     * @throws IllegalArgumentException
     *             if <tt>fromValue</tt> follows <tt>toValue</tt>.
     */
    public static Term rangeTerm(Object fromValue, Object toValue) {
        return rangeTerm(fromValue, toValue, true, true, null);
    }

    /**
     * Returns a <tt>Term</tt> that accepts a range of values. The range
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
     *            A <tt>CoderContext</tt> supplied to any registered
     *            {@link com.persistit.encoding.KeyCoder} used in encoding the
     *            <tt>fromValue</tt> or <tt>toValue</tt>. May be <tt>null</tt>.
     * 
     * @return The <tt>term</tt>
     * 
     * @throws IllegalArgumentException
     *             if <tt>fromValue</tt> follows <tt>toValue</tt>.
     */
    public static Term rangeTerm(Object fromValue, Object toValue,
            CoderContext context) {
        return rangeTerm(fromValue, toValue, true, true, context);
    }

    /**
     * Returns a <tt>Term</tt> that accepts a range of values. The range
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
     *            Indicates whether a value exactly matching <tt>fromValue</tt>
     *            should be selected by this <tt>Term</tt>.
     * 
     * @param rightInclusive
     *            Indicates whether a value exactly matching <tt>toValue</tt>
     *            should be selected by this <tt>Term</tt>.
     * 
     * @return The <tt>term</tt>
     * 
     * @throws IllegalArgumentException
     *             if <tt>fromValue</tt> follows <tt>toValue</tt>.
     */

    public static Term rangeTerm(Object fromValue, Object toValue,
            boolean leftInclusive, boolean rightInclusive) {
        return rangeTerm(fromValue, toValue, leftInclusive, rightInclusive,
                null);
    }

    /**
     * Returns a <tt>Term</tt> that accepts a range of values. The range
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
     *            Indicates whether a value exactly matching <tt>fromValue</tt>
     *            should be selected by this <tt>Term</tt>.
     * 
     * @param rightInclusive
     *            Indicates whether a value exactly matching <tt>toValue</tt>
     *            should be selected by this <tt>Term</tt>.
     * 
     * 
     * @param context
     *            A <tt>CoderContext</tt> supplied to any registered
     *            {@link com.persistit.encoding.KeyCoder} used in encoding the
     *            <tt>fromValue</tt> or <tt>toValue</tt>. May be <tt>null</tt>.
     * 
     * @return The <tt>term</tt>
     * 
     * @throws IllegalArgumentException
     *             if <tt>fromValue</tt> follows <tt>toValue</tt>.
     */
    public static Term rangeTerm(Object fromValue, Object toValue,
            boolean leftInclusive, boolean rightInclusive, CoderContext context) {
        Key key = new Key((Persistit) null);
        if (fromValue == null)
            key.appendBefore();
        else
            key.append(fromValue, context);
        key.reset();
        byte[] leftBytes = segmentBytes(key);
        key.clear();
        if (toValue != fromValue && toValue != null) {
            key.append(toValue, context);
            key.reset();
            byte[] rightBytes = segmentBytes(key);
            if (compare(leftBytes, rightBytes) > 0) {
                throw new IllegalArgumentException("Start value \"" + fromValue
                        + "\" is after end value \"" + toValue + "\".");
            }
            return new RangeTerm(leftBytes, rightBytes, leftInclusive,
                    rightInclusive);
        } else
            return new SimpleTerm(leftBytes);
    }

    /**
     * Returns a <tt>Term</tt> that accepts a range of values. The range is
     * specified by values already encoded in two supplied {@link Key}s. The
     * index of each Key object should be set on entry to the segment to be used
     * in constructing the RangeTerm. As a side-effect, the index of each key is
     * advanced to the next segment. If the two key segments are identical and
     * if both leftInclusive and rightInclusive are true, this method returns a
     * SimpleTerm containing the segment.
     * 
     * @param fromKey
     *            A <tt>Key</tt? from which the low value in the range is
     *            extracted
     * 
     * @param toKey
     *            A <tt>Key</tt? from which the high value in the range is
     *            extracted
     * 
     * @param leftInclusive
     *            Indicates whether a value exactly matching <tt>fromValue</tt>
     *            should be selected by this <tt>Term</tt>.
     * 
     * @param rightInclusive
     *            Indicates whether a value exactly matching <tt>toValue</tt>
     *            should be selected by this <tt>Term</tt>.
     * 
     * @return The <tt>term</tt>
     */
    public static Term termFromKeySegments(Key fromKey, Key toKey,
            boolean leftInclusive, boolean rightInclusive) {
        byte[] leftBytes = segmentBytes(fromKey);
        byte[] rightBytes = segmentBytes(toKey);
        toKey.nextElementIndex();
        if (leftInclusive && rightInclusive
                && compare(leftBytes, rightBytes) == 0) {
            return new SimpleTerm(leftBytes);
        } else {
            return new RangeTerm(leftBytes, rightBytes, leftInclusive,
                    rightInclusive);
        }
    }

    /**
     * Returns a <tt>Term</tt> that selects a key segment value if and only if
     * one of the members of the supplied <tt>terms</tt> array selects it. The
     * <tt>terms</tt> array may not include a nested <tt>OrTerm</tt>.
     * 
     * @param terms
     *            Array of <tt>RangeTerm</tt>s or <tt>SimpleTerm</tt>s.
     * 
     * @return The <tt>term</tt>
     * 
     * @throws IllegalArgumentException
     *             if any member of the <tt>terms</tt> array is itself an
     *             <tt>OrTerm</tt> or if the end points of the terms in that
     *             array are not strictly increasing in <a
     *             href="Key.html#_keyOrdering">key order</a>.
     */
    public static Term orTerm(Term[] terms) {
        return new OrTerm(terms);
    }

    static byte[] segmentBytes(Key key) {
        int from = key.getIndex();
        int to = key.nextElementIndex();
        if (to < 0)
            to = key.getEncodedSize();
        if (to <= 0)
            throw new MissingKeySegmentException();
        byte[] bytes = new byte[to - from];
        System.arraycopy(key.getEncodedBytes(), from, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Returns the current size of this <tt>KeyFilter</tt>'s term array.
     * 
     * @return The size.
     */
    public int size() {
        return _terms.length;
    }

    /**
     * Returns the minimum {@link Key#getDepth() depth} of a key value that will
     * be selected by this <tt>KeyFilter</tt>.
     * 
     * @return The minimum depth
     */
    public int getMinimumDepth() {
        return _minDepth;
    }

    /**
     * Returns the maximum {@link Key#getDepth() depth} of a key value that will
     * be selected by this <tt>KeyFilter</tt>.
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
     * @return The <tt>Term</tt>.
     * 
     * @throws ArrayIndexOutOfBoundsException
     *             if <tt>index</tt> is less than zero or greater than or equal
     *             to the number of terms in the term array.
     */
    public Term getTerm(int index) {
        return _terms[index];
    }

    /**
     * Returns a <a href="#_stringRepresentation">string representation</a> of
     * this <tt>KeyFilter</tt>
     * 
     * @return The canonical string representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
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
     *            The <tt>Key</tt> value to test.
     * 
     * @return <tt>true</tt> if the supplied <tt>Key</tt> state satisfies the
     *         constraints of this filter, otherwise <tt>false</tt>.
     */
    public boolean selected(Key key) {

        int index = 0;
        int size = key.getEncodedSize();
        byte[] keyBytes = key.getEncodedBytes();

        for (int level = 0; ; level++) {
            if (index == size) {
                return (level >= _minDepth);
            } else if (level >= _maxDepth) {
                return false;
            } else {
                int nextIndex = key.nextElementIndex(index);
                if (nextIndex == -1) {
                    nextIndex = key.getEncodedSize();
                }
                Term term = level < _terms.length ? _terms[level] : ALL;
                if (!term.selected(keyBytes, index, nextIndex - index)) {
                    return false;
                }
                index = nextIndex;
            }
        }
    }

    /**
     * Given a key value, determines whether the adjacent key (as determined by
     * the Direction and the value of the deep flag) is selected by this
     * KeyFilter. If not, the KeyFilter range is <i>exhausted</i> and the
     * {@link #traverse(Key, boolean)} method must be used to find the next
     * possible range of selected keys, if there are any.
     * <p />
     * More formally, if there is no B-Tree for which the result of
     * {@link com.persistit.Exchange#traverse(com.persistit.Key.Direction, boolean)}
     * can be selected, then the KeyFilter is <i>exhausted</i> for that Key,
     * Direction and deep value.
     * 
     * @param key
     *            the Key
     * @param direction
     *            the Direction
     * @param deep
     *            whether this is a deep (depth-first) traversal
     * @return <tt>true</tt> if the filter is exhausted.
     */
    public boolean exhausted(Key key, Key.Direction direction) {
        int index = 0;
        int size = key.getEncodedSize();
        byte[] keyBytes = key.getEncodedBytes();
        boolean exhausted = true;

        for (int level = 0; ; level++) {
            if (index == size) {
                if (level < _minDepth) {
                    return true;
                } else {
                    return exhausted;
                }
            } else if (level >= _maxDepth) {
                return exhausted;
            } else {
                int nextIndex = key.nextElementIndex(index);
                if (nextIndex == -1) {
                    nextIndex = key.getEncodedSize();
                }
                Term term = level < _terms.length ? _terms[level] : ALL;
                exhausted &= term.exhausted(keyBytes, index, nextIndex - index, direction);
                index = nextIndex;
            }
        }
    }

    /**
     * <p>
     * Determines a key value that is <i>adjacent</i> to the next or previous
     * (depending on the supplied value of <tt>forward</tt>) key value in the
     * subset of all keys selected by this <tt>KeyFilter</tt>. This method
     * modifies the state of the supplied <tt>key</tt> to reflect that next or
     * previous adjacent value if there is one. The returned <tt>boolean</tt>
     * value indicates whether such a key exists in the filtered subset.
     * </p>
     * <p>
     * Suppose <tt>key</tt> has some value <i>K</i>. Then if <i>K</i> is
     * selected by this <tt>KeyFilter</tt>, this method returns <tt>true</tt>
     * and modifies the value of <tt>key</tt>, to the next (or previous, if
     * <tt>forward</tt> is <tt>false</tt>) key value <i>K'</i> such that there
     * is no other key value between <i>K</i> and <i>K'</i> in <a
     * href="Key.html#_keyOrdering">key order</a>.
     * </p>
     * <p>
     * If <i>K</i> is not selected by this <tt>KeyFilter</tt>, then let <i>J</i>
     * be the smallest key value larger than than <i>K</i> (or if
     * <tt>forward</tt> is <tt>false</tt>, the largest key value smaller than
     * <i>K</i>) that is selected by this <tt>KeyFilter</tt>. If no such key
     * value <i>J</i> exists then this method returns <tt>false</tt>. Otherwise
     * it modifies the value of the supplied <tt>key</tt> and returns
     * <tt>true</tt>. The new key value is <i>J'</i> where <i>J'</i> is the
     * largest key value smaller than <i>J</i> if <tt>forward</tt> is
     * <tt>true</tt>, or the smallest key value larger than <i>J</i> if
     * <tt>forward</tt> is <tt>false</tt>.
     * </p>
     * <p>
     * Note that this method does not necessarily find a key that actually
     * exists in a Persistit tree, but rather a key value from which traversal
     * in a tree can proceed. The
     * {@link Exchange#traverse(Key.Direction, KeyFilter, int)} method uses this
     * method to perform efficient filtered key traversal.
     * </p>
     * 
     * @param key
     *            The <tt>Key</tt>
     * 
     * @param forward
     *            <tt>true</tt> to advance to the next larger key, or
     *            <tt>false</tt> to advance to the next smaller key within the
     *            key ordering.
     * 
     * @return <tt>true</tt> if a successor (or predecessor) key exists,
     *         otherwise <tt>false</tt>.
     */
    public boolean traverse(Key key, boolean forward) {
        if (key.getEncodedSize() == 0) {
            if (forward) {
                key.appendBefore();
            } else {
                key.appendAfter();
            }
        }
        final boolean result = traverse(key, 0, 0, forward);
        return result;
    }

    private boolean traverse(Key key, int index, int level, boolean forward) {
        if (Debug.ENABLED) {
            Debug.$assert(level < _maxDepth);
            Debug.$assert(index < key.getEncodedSize());
        }
        int nextIndex = key.nextElementIndex(index);
        if (nextIndex == -1) {
            nextIndex = key.getEncodedSize();
        }
        final boolean lastKeySegment = nextIndex == key.getEncodedSize();

        Term term = level >= _terms.length ? ALL : _terms[level];

        boolean traversed = false;
        if (forward) {
            if (level + 1 < _maxDepth) {
                if (term.selected(key.getEncodedBytes(), index, nextIndex
                        - index)) {
                    if (lastKeySegment && !key.isSpecial()) {
                        key.appendBefore();
                        traversed = traverse(key, nextIndex, level + 1, true);
                    } else if (!lastKeySegment) {
                        traversed = traverse(key, nextIndex, level + 1, true);
                    }
                }
            }
            if (!traversed) {
                traversed = term.forward(key, index, nextIndex - index);
                nextIndex = key.getEncodedSize();
                if (traversed
                        && level + 1 < _minDepth
                        && level < _terms.length
                        && !key.isSpecial()
                        && term.selected(key.getEncodedBytes(), index,
                                nextIndex - index)) {
                    key.appendBefore();
                    traversed = traverse(key, nextIndex, level + 1, true);
                }
            }
        } else {
            if (level + 1 < _maxDepth) {
                if (key.getEncodedSize() > nextIndex
                        && term.selected(key.getEncodedBytes(), index,
                                nextIndex - index)) {
                    traversed = traverse(key, nextIndex, level + 1, false);
                }
            } else if (!lastKeySegment && level + 1 == _maxDepth) {
                key.setEncodedSize(nextIndex);
                traversed = term.selected(key.getEncodedBytes(), index,
                        nextIndex - index);
            }
            if (!traversed) {
                traversed = term.backward(key, index, nextIndex - index);
                nextIndex = key.getEncodedSize();
                if (traversed
                        && level + 1 < _maxDepth
                        && !key.isSpecial()
                        && term.selected(key.getEncodedBytes(), index,
                                nextIndex)) {
                    key.appendAfter();
                    traversed = traverse(key, nextIndex, level + 1, false);
                }
            }
        }
        return traversed;
    }

    private static int compare(byte[] a, byte[] b) {
        if (a == b)
            return 0;
        if (a == null && b != null)
            return Integer.MIN_VALUE;
        if (a != null && b == null)
            return Integer.MAX_VALUE;
        return compare(a, 0, a.length, b, 0, b.length);
    }

    private static int compare(byte[] a, int offsetA, int sizeA, byte[] b,
            int offsetB, int sizeB) {
        int size = Math.min(sizeA, sizeB);
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

    private static int byteHash(byte[] a) {
        if (a == null)
            return 0;
        int h = 0;
        for (int i = 0; i < a.length; i++) {
            h = h * 17 + a[i];
        }
        return h;
    }

    private static void appendDisplayableKeySegment(Key workKey,
            StringBuilder sb, byte[] bytes, CoderContext context,
            boolean before, boolean after) {
        if (bytes == null)
            return;
        System.arraycopy(bytes, 0, workKey.getEncodedBytes(), 0, bytes.length);
        workKey.setEncodedSize(bytes.length);
        if (before && workKey.isBefore() || after && workKey.isAfter()) {
            return;
        }

        try {
            workKey.decodeDisplayable(true, sb, context);
        } catch (Exception e) {
            sb.append(e);
            sb.append("(");
            sb.append(Util.hexDump(bytes, 0, bytes.length));
            sb.append(")");
        }
    }
}
