package com.persistit;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BufferMaxPack {

    @Test
    public void testJoinBuffer() throws Exception {
        final Persistit db = new Persistit();
        final Buffer b1 = new Buffer(16384, 0, null, db);
        b1.init(Buffer.PAGE_TYPE_DATA);
        b1.claim(true);
        final Key key = new Key((Persistit) null);
        final Value value = new Value((Persistit) null);
        for (int i = 0; i < 4096; i++) {
            key.clear().append(i);
            int at = b1.putValue(key, value);
            if (at < 0) {
                break;
            }
        }
        b1.invalidateFindex();
        b1.recomputeFindex();
        assertTrue(b1.getKeyCount() < b1.getBufferSize() / 16);
    }
}
