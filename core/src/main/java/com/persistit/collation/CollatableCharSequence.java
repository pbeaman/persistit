/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.collation;

/**
 * Convert string-valued data to and from a form that sorts correctly under a
 * binary ordering for use in B-Tree keys. This interface defines two main
 * methods, {@link #factor(byte[], int, int, byte[], int, int)} and
 * {@link #combine(byte[], int, int, byte[], int, int)}. These methods are
 * responsible for encoding and decoding strings within Persistit {@link Key}
 * instances in such a way that binary comparisons of the key bytes will result
 * in correct locale-specific ordering of the keys.
 * 
 * @author peter
 * 
 */
public interface CollatableCharSequence extends CharSequence, Appendable {

    /**
     * <p>
     * Encode a string value for proper collation. This method converts a String
     * into two byte arrays. One is an encoding of a derived sort key for the
     * string (the "keyBytes") and the other (the "caseBytes") contains the
     * information needed to recover the original string when combined with the
     * keyBytes.
     * </p>
     * <p>
     * An implementation of this method produces a sort key which when compared
     * to sort keys for other string values, produces the correct ordering for a
     * collation scheme. Implementations will embody the logic of the Unicode
     * Collation Architecture to produce locale-sensitive collation of strings.
     * </p>
     * <p>
     * Separately, an implementation also optionally produces the caseBytes, an
     * additional byte array that can subsequently be combined with the keyBytes
     * to recreate the original string value. This feature is optional because
     * some use cases do not require reconstructing the original key. For
     * example, an index used only for lookup and sorting, but never to retrieve
     * values, does not need to retain the caseBytes.
     * </p>
     * <p>
     * It is intended that the supplied collationId is a small-integer
     * identifier for a particular encoding scheme. The implementation of this
     * method should encode this value into the caseBytes so that it can be
     * recovered by the {@link #combine(byte[], int, int, byte[], int, int)}
     * method. CollationId 0 is reserved for default UTF-8 encoding without
     * collation. That is, the result of the factor operation when collationId
     * is 0 is equivalent to Persistit's default UTF-8 key encoding for strings
     * and length of the resulting caseBytes array is zero.
     * </p>
     * <p>
     * The return value is a long that holds the length of the case bytes in its
     * upper 32 bits and the length of the keyBytes in its lower 32 bits. The
     * method writes the keyBytes and caseBytes into the supplied byte arrays
     * with specified upper bounds on sizes for each. If the length is exceeded,
     * this method throws an ArrayIndexOutOfBoundsException.
     * </p>
     * 
     * @param collationId
     *            An integer-valued identifier for the particular collation
     *            sequence used to perform the encoding. Zero is a reserved
     *            value. Values must be non-negative.
     * @param keyBytes
     *            Byte array into which keyBytes are written
     * @param keyBytesOffset
     *            offset of first byte of keyBytes
     * @param keyBytesMaximumLength
     *            maximum permissible length of keyBytes
     * @param caseBytes
     *            Byte array into which caseBytes are optionally written
     * @param caseBytesOffset
     *            offset of first byte of caseBytes
     * @param caseBytesMaximumLength
     *            maximum permissible length of caseBytes, or zero to avoid
     *            producing them
     * @return ((caseBytes length) << 32) + (keyBytes length)
     * @throws ArrayIndexOutOfBoundsException
     *             if either maximum array length is exceeded
     * @throws IllegalArgumentException
     *             if the implementation determines that the string it
     *             represents cannot be encoded
     */
    long factor(int collationId, byte[] keyBytes, int keyBytesOffset, int keyBytesMaximumLength, byte[] caseBytes,
            int caseBytesOffset, int caseBytesMaximumLength);

    /**
     * <p>
     * Combine the supplied byte arrays to produce a string. See
     * {@link #factor(int, byte[], int, int, byte[], int, int)} for descriptions
     * of the keyBytes and caseBytes arrays. The result of calling this method
     * is equivalent to calling the {@link #append(char)} method for each
     * character produced by combining the two byte arrays.
     * </p>
     * <p>
     * It is intended that the caseBytes should encode the collationId value
     * used when the factor method was called. This method verifies that the
     * encoded value matches the supplied value (if caseBytes is not empty). If
     * caseBytes is empty then the combine operation uses the collation
     * identified by the supplied collationId, but results in a string from
     * which some information has been removed (for example, case). If the
     * supplied collationId value is -1 then the collation is determined by the
     * supplied caseBytes without verification. And finally, if the supplied
     * value is -1 or 0 and the supplied caseBytes is empty, then this method
     * attempts to decode the keyBytes using Persistit's default UTF-8 key
     * encoding scheme.
     * </p>
     * 
     * @param collationId
     *            The collationId with which the caseBytes were previously
     *            created, or -1 to decode and use the collationId encoded in
     *            the caseBytes without verification. The value 0 is reserved to
     *            indicate Persistit's default UTF-8 encoding scheme.
     * @param keyBytes
     *            Byte array containing keyBytes
     * @param keyBytesOffset
     *            offset of first byte of keyBytes
     * @param keyBytesLength
     *            length of keyBytes
     * @param caseBytes
     *            Byte array containing caseBytes
     * @param caseBytesOffset
     *            offset of first byte of caseBytes
     * @param caseBytesLength
     *            length of caseBytes
     * @return length of the resulting string
     */
    int combine(int collationId, byte[] keyBytes, int keyBytesOffset, int keyBytesLength, byte[] caseBytes,
            int caseBytesOffset, int caseBytesLength);

    /**
     * Decode the collationId from the supplied caseBytes.
     * 
     * @param caseBytes
     *            Byte array containing caseBytes
     * @param caseBytesOffset
     *            offset of first byte of caseBytes
     * @param caseBytesLength
     *            length of caseBytes
     * @return the collationId encoded in the supplied caseBytes, or zero if the
     *         caseBytesLength is zero.
     */
    int decodeCollationId(byte[] caseBytes, int caseBytesOffset, int caseBytesLength);
}
