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

package com.persistit;

import com.persistit.Exchange.Sequence;
import com.persistit.Management.RecordInfo;
import com.persistit.ValueHelper.RawValueWriter;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RebalanceException;
import com.persistit.policy.JoinPolicy;
import com.persistit.policy.SplitPolicy;
import com.persistit.unit.PersistitUnitTestCase;

public class BufferTest extends PersistitUnitTestCase {
    int leftn;
    int rightn;
    int leftklen;
    int rightklen;
    int leftvlen;
    int rightvlen;
    int leftShorten;
    int rightShorten;

    public void testSimpleSplitAndJoin() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "BufferTest", true);
        final StringBuilder sb = new StringBuilder();
        final Buffer b1 = ex.getBufferPool().get(ex.getVolume(), 1, true, false);
        final Buffer b2 = ex.getBufferPool().get(ex.getVolume(), 2, true, false);
        RawValueWriter vwriter = new RawValueWriter();
        b1.init(Buffer.PAGE_TYPE_DATA);
        b2.init(Buffer.PAGE_TYPE_DATA);
        b1.getFastIndex();
        b2.getFastIndex();
        final Key key = new Key((Persistit) null);
        final Key indexKey = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);

        for (int i = 'a'; i < 'z'; i++) {
            sb.append((char) i).append((char) i);
            key.to(sb);
            value.putString(sb);
            vwriter.init(value);
            b1.putValue(key, vwriter);
        }

        sb.setLength(20);
        key.to(sb);
        final int foundAt = b1.findKey(key);
        b1.split(b2, key, vwriter, foundAt, indexKey, Sequence.NONE, SplitPolicy.NICE_BIAS);

        final RecordInfo info = b2.getRecords()[0];
        info.getKeyState().copyTo(key);
        final Key key1 = new Key(key);
        final Key key2 = new Key(key);
        key1.nudgeLeft();
        key2.nudgeRight();
        final int foundAt1 = b1.findKey(key1);
        final int foundAt2 = b2.findKey(key2);
        b1.join(b2, foundAt1, foundAt2, key1, key2, JoinPolicy.EVEN_BIAS);

        final RecordInfo r = b1.getRecords()[b1.getRecords().length - 1];
        r.getKeyState().copyTo(key);
        assertTrue(key.toString().contains("ttuuvv"));
        b1.release();
        b2.release();
    }

    public void testProblematicJoins() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "BufferTest", true);
        final Buffer b1 = ex.getBufferPool().get(ex.getVolume(), 1, true, false);
        final Buffer b2 = ex.getBufferPool().get(ex.getVolume(), 2, true, false);

        leftn = 200;
        rightn = 200;
        leftklen = 20;
        rightklen = 20;
        leftvlen = 20;
        rightvlen = 20;
        leftShorten = 0;
        rightShorten = 0;
        combo(ex, b1, b2);

        leftn = 1000;
        rightn = 1000;
        combo(ex, b1, b2);

        leftShorten = 2;
        rightShorten = 7;
        combo(ex, b1, b2);

    }

    /*
     * Note: runs for about 3 minutes -- ignored for now
     */
    public void manyJoins() throws Exception {
        final Exchange ex = _persistit.getExchange("persistit", "BufferTest", true);
        final Buffer b1 = ex.getBufferPool().get(ex.getVolume(), 1, true, false);
        final Buffer b2 = ex.getBufferPool().get(ex.getVolume(), 2, true, false);

        for (leftn = 200; leftn < 800; leftn += 200) {
            for (rightn = 200; rightn < 800; rightn += 200) {
                for (leftklen = 5; leftklen < 40; leftklen += 10) {
                    for (rightklen = 5; rightklen < 40; rightklen += 10) {
                        for (leftvlen = 10; leftvlen <= 100; leftvlen += 10) {
                            for (rightvlen = 10; rightvlen <= 100; rightvlen += 30) {
                                for (leftShorten = 0; leftShorten < 10; leftShorten += 3) {
                                    for (rightShorten = 0; rightShorten < 10; rightShorten += 3) {
                                        combo(ex, b1, b2);
                                    }
                                }
                            }

                        }
                    }
                }
            }
        }

    }

    private boolean combo(Exchange ex, Buffer b1, Buffer b2) throws Exception {

        try {
            b1.init(Buffer.PAGE_TYPE_DATA);
            b1.clearSlack();
            b2.init(Buffer.PAGE_TYPE_DATA);
            b2.clearSlack();

            int n1 = fillPage(ex, b1, 1, leftn, leftklen, leftvlen);
            fillPage(ex, b2, n1, 1, leftklen, leftvlen);
            int n2 = fillPage(ex, b2, n1 + 1, rightn, rightklen, rightvlen);

            if (leftShorten > 0) {
                fillPage(ex, b1, 1, n1 - 1, leftklen, leftvlen - leftShorten);
            }

            if (rightShorten > 0) {
                fillPage(ex, b2, n1 + 1, n2 - n1 - 1, rightklen, rightvlen - rightShorten);
            }

            b1.verify(null, null);
            b2.verify(null, null);

            joinPages(ex, b1, b2, n1, n1);

            b1.verify(null, null);
            b2.verify(null, null);
            return false;
        } catch (RebalanceException e) {
            return true;
        }
    }

    private int fillPage(Exchange ex, Buffer b, int k, int n, int klen, int vlen) throws PersistitException {
        RawValueWriter vwriter = new RawValueWriter();
        vwriter.init(ex.getValue());
        StringBuilder sb = new StringBuilder(vlen);
        while (sb.length() < vlen) {
            sb.append(RED_FOX);
        }
        sb.setLength(vlen);
        ex.getValue().put(sb.toString());
        int i = -1;
        int offset = 0;
        for (i = k; i < k + n; i++) {
            ex.getKey().clear().append(String.format("%05d%" + klen + "s", i, ""));
            offset = b.putValue(ex.getKey(), vwriter);
            if (offset == -1) {
                break;
            }
        }
        for (int j = i; j >= i - 1; j--) {
            ex.getValue().clear();
            ex.getKey().clear().append(String.format("%05d%" + klen + "s", j, ""));
            offset = b.putValue(ex.getKey(), vwriter);
            if (offset != -1) {
                return j;
            }
        }
        return -1;
    }

    private void joinPages(Exchange ex, Buffer b1, Buffer b2, int key1, int key2) throws PersistitException {
        ex.getKey().clear().append(String.format(String.format("%05d%" + leftklen + "s", key1, "")));
        int foundAt1 = b1.findKey(ex.getKey());
        ex.getKey().clear().append(String.format(String.format("%05d%" + leftklen + "s", key2, "")));
        int foundAt2 = b2.findKey(ex.getKey());
        if ((foundAt2 & Buffer.EXACT_MASK) != 0) {
            foundAt2 += 3;
        }
        b1.join(b2, foundAt1, foundAt2, ex.getAuxiliaryKey1(), ex.getAuxiliaryKey2(), JoinPolicy.EVEN_BIAS);
    }

    /*
     * ----------
     */

    String keyString(char fill, int length, int prefix, int width, int k) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < prefix && i < length; i++) {
            sb.append(fill);
        }
        sb.append(String.format("%0" + width + "d", k));
        for (int i = length - sb.length(); i < length; i++) {
            sb.append(fill);
        }
        sb.setLength(length);
        return sb.toString();
    }

}
