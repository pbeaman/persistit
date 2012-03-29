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

import org.junit.Test;

import com.persistit.ValueHelper.RawValueWriter;
import com.persistit.unit.PersistitUnitTestCase;

public class BufferMaxPack extends PersistitUnitTestCase {

    @Test
    public void testJoinBuffer() throws Exception {
        final Buffer b1 = _persistit.getBufferPool(16384).get(_persistit.getVolume("persistit"), 1, true, false);
        b1.init(Buffer.PAGE_TYPE_DATA);
        b1.claim(true);
        final Key key = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);
        RawValueWriter vwriter = new RawValueWriter();
        vwriter.init(value);
        for (int i = 0; i < 4096; i++) {
            key.clear().append(i);
            int at = b1.putValue(key, vwriter);
            if (at < 0) {
                break;
            }
        }

        assertTrue(b1.getFastIndex() != null);
        b1.getFastIndex().invalidate();
        b1.getFastIndex().recompute();
        assertTrue(b1.getFastIndex().isValid());
        assertTrue(b1.getKeyCount() < b1.getBufferSize() / 16);
    }
}
