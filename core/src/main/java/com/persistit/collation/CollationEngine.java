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

public interface CollationEngine {

    /**
     * @return the unique collationId of this CollationEngine implementation
     */
    public int getCollationId();

    /**
     * <p>
     * Encode a string value for proper collation using the supplied array
     * specifications to hold the keyBytes and caseBytes. The encoding scheme is
     * specified by the collationId value last assigned via
     * {@link #setCollationId(int)}.
     * </p>
     * <p>
     * An implementation of this method may only use non-zero bytes to encode
     * the keyBytes field. Zero bytes are reserved to delimit the ends of key
     * segments. Since this method is used in performance-critical code paths,
     * the encode produced by this method is not verified for correctness.
     * Therefore an incorrect implementation will produce anomalous results.
     * </p>
     * <p>
     * The return value is a long that holds the length of the case bytes in its
     * upper 32 bits and the length of the keyBytes in its lower 32 bits. The
     * method writes the keyBytes and caseBytes into the supplied byte arrays
     * with specified upper bounds on sizes for each. If the length is exceeded,
     * this method throws an ArrayIndexOutOfBoundsException.
     * </p>
     * 
     * @param source
     *            a CharSequence from which the string to be encoded is supplied
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
    long encode(CharSequence source, byte[] keyBytes, int keyBytesOffset, int keyBytesMaximumLength, byte[] caseBytes,
            int caseBytesOffset, int caseBytesMaximumLength);

    /**
     * <p>
     * Combine the supplied byte arrays previously produced by the
     * {@link #encode} method to produce a string. The result of calling this
     * method is equivalent to calling the {@link #append(char)} method for each
     * character produced by combining the two byte arrays.
     * </p>
     * <p>
     * It is intended that the caseBytes should encode the collationId value
     * used when the <code>encode</code> method was called. That value (E)
     * interacts with the current value of {@link #getCollationId()} (C) in the
     * following way:
     * <ul>
     * <li>if E == C then the string is decoded using the collation scheme
     * identified by E.</li>
     * <li>if E is non-zero and C is -1 then the string is decoded using the
     * collation scheme identified by E and the current collationId of this
     * CollatableCharSequence is set to E as if setCollationId(E) were called.</li>
     * <li>if E>0, C>0 and E != C then this method throws an
     * IllegalArgumentException.
     * <li>if caseBytes is empty then the collation scheme identified by C is
     * used to decode the string, but the result is likely to be different from
     * the original string due to missing information.</li>
     * <li>if caseBytes is empty and C is -1 or 0 then the default Persistit
     * UTF-8 binary encoding is used to create a string from the keyBytes</li>
     * </ul>
     * </p>
     * 
     * @param target
     *            An Appendable to which the decoded string value will be
     *            written
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
     * @throws IllegalArgumentException
     *             if the collationId encoded in caseBytes does not match the
     *             current value of {@link #getCollationId()}.
     */
    int decode(Appendable target, byte[] keyBytes, int keyBytesOffset, int keyBytesLength, byte[] caseBytes,
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

    /**
     * Compare the strings represented by source and target using the collation
     * rules of this CollationEngine.
     * 
     * @param source
     *            first string to compare
     * @param other
     *            second string to compare
     * @return a negative integer, zero, or a positive integer as the source
     *         string is less than, equal to, or greater than the target string
     *         under the collation rules of this CollationEngine
     */
    int compare(CharSequence source, CharSequence other);

}
