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
 * A CollatableCharSequence that delegates to a StringBuilder and uses
 * a collationId to find the correct CollationEngine.
 * 
 * @author peter
 */
public class CollatableStringBuilder implements CollatableCharSequence<CharSequence>, Appendable {

    private final StringBuilder _sb;

    private final int _collationId;

    private final CollationEngine _collationEngine;

    private final boolean _recoverableEncoding;

    public CollatableStringBuilder(final StringBuilder sb, final int collationId, final boolean recoverable) {
        _sb = sb;
        _collationId = collationId;
        _recoverableEncoding = recoverable;
        _collationEngine = CollationEngines.getEngine(_collationId);
    }

    @Override
    public String toString() {
        return _sb.toString();
    }

    @Override
    public int length() {
        return _sb.length();
    }

    @Override
    public char charAt(int index) {
        return _sb.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return _sb.subSequence(start, end);
    }

    @Override
    public int getCollationId() {
        return _collationId;
    }

    @Override
    public long encode(byte[] keyBytes, int keyBytesOffset, int keyBytesMaximumLength, byte[] caseBytes,
            int caseBytesOffset, int caseBytesMaximumLength) {
        return _collationEngine.encode(_sb, keyBytes, keyBytesOffset, keyBytesMaximumLength, caseBytes,
                caseBytesOffset, _recoverableEncoding ? caseBytesMaximumLength : 0);
    }

    @Override
    public Appendable append(CharSequence csq) {
        return _sb.append(csq);
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) {
        return _sb.append(csq, start, end);
    }

    @Override
    public Appendable append(char c) {
        return _sb.append(c);
    }

    public void setLength(final int newLength) {
        _sb.setLength(newLength);
    }

    /**
     * @return a negative integer, zero, or a positive integer as the string
     *         string represented by this object is less than, equal to, or
     *         greater than the target string under the collation rules implied
     *         by the current collationId
     */
    @Override
    public int compareTo(CharSequence target) {
        return _collationEngine.compare(this, target);
    }

}
