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
 * <p>
 * Convert string-valued data to and from a form that sorts correctly under a
 * binary ordering for use in B-Tree keys. This interface defines two main
 * methods, {@link #encode(byte[], int, int, byte[], int, int)} and
 * {@link #decode(byte[], int, int, byte[], int, int)}. These methods are
 * responsible for encoding and decoding strings within Persistit {@link Key}
 * instances in such a way that binary comparisons of the key bytes will result
 * in correct locale-specific ordering of the keys.
 * </p>
 * 
 * <p>
 * The <code>encode</code> method populates two byte arrays, one containing the
 * "keyBytes" and the other containing the "caseBytes". The keyBytes array is an
 * encoding of a derived sort key for the string. The caseBytes contains the
 * information needed to recover the original string when combined with the
 * keyBytes.
 * </p>
 * 
 * <p>
 * As might be expected, the <code>decode</code> method combines the keyBytes
 * and caseBytes data to recreate the original string.
 * </p>
 * 
 * <p>
 * An instance of this interface holds a small-integer collationId. Its value is
 * intended to be used as a handle by which an implementation of a specific
 * collation scheme can be looked up. CollationId value zero is reserved to
 * specify collation consistent with the default UTF-8 encoding scheme. Small
 * positive integers are used to specify collation schemes for supported
 * languages.
 * </p>
 * 
 * <p>
 * You must specify the collationId before calling the <code>encode</code>
 * method. The implementation of <code>encode</code> should encode the
 * collationId into the caseBytes array.
 * </p>
 * 
 * <p>
 * You may specify the collationId before calling the <code>decode</code>
 * method, in which case the value encoded in the caseBytes will be verified
 * against the supplied value. Alternatively you may specify a value of -1 in
 * which case the value encoded in caseBytes will be used without verification.
 * </p>
 * 
 * <p>
 * As an option specified by passing a value of zero as caseBytesMaximumLength,
 * the <code>encode</code> method may not write any case bytes. In this case,
 * the value of {@link #getCollationId()} will be used when decoding the string.
 * </p>
 * 
 * @author peter
 * 
 */
public interface CollatableCharSequence<T extends CharSequence> extends CharSequence, Comparable<T> {

    /**
     * @return the current collationId
     */
    int getCollationId();

    /**
     * <p>
     * Encode a string value for proper collation using the supplied array
     * specifications to hold the keyBytes and caseBytes. The encoding scheme is
     * specified by the collationId value last assigned via
     * {@link #setCollationId(int)}.
     * </p>
     * <p>
     * An implementation of this method may only use non-zero bytes to encode the
     * keyBytes field. Zero bytes are reserved to delimit the ends of key segments.
     * Since this method is used in performance-critical code paths, the encode
     * produced by this method is not verified for correctness. Therefore an incorrect
     * implementation will produce anomalous results. 
     * </p>
     * <p>
     * The return value is a long that holds the length of the case bytes in its
     * upper 32 bits and the length of the keyBytes in its lower 32 bits. The
     * method writes the keyBytes and caseBytes into the supplied byte arrays
     * with specified upper bounds on sizes for each. If the length is exceeded,
     * this method throws an ArrayIndexOutOfBoundsException.
     * </p>
     * 
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
    long encode(byte[] keyBytes, int keyBytesOffset, int keyBytesMaximumLength, byte[] caseBytes, int caseBytesOffset,
            int caseBytesMaximumLength);

}
