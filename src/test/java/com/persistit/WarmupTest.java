/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class WarmupTest extends PersistitUnitTestCase {

    @Override
    protected Properties getProperties(final boolean cleanup) {
        final Properties p = super.getProperties(cleanup);
        p.setProperty("bufferinventory", "true");
        p.setProperty("bufferpreload", "true");
        return p;
    }

    @Test
    public void testWarmup() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "WarmupTest", true);
        final BufferPool pool = ex.getBufferPool();
        for (int i = 1; i <= 1000; i++) {
            ex.getValue().put(RED_FOX);
            ex.clear().append(i).store();
        }

        final Buffer[] buff = new Buffer[100];
        for (int i = 0; i < pool.getBufferCount(); ++i) {
            buff[i] = pool.getBufferCopy(i);
        }

        final Configuration config = _persistit.getConfiguration();
        ex = null;
        _persistit.close();

        _persistit = new Persistit();
        _persistit.initialize(config);

        for (int i = 0; i < pool.getBufferCount(); ++i) {
            final Buffer bufferCopy = pool.getBufferCopy(i);
            assertEquals(bufferCopy.getPageAddress(), buff[i].getPageAddress());
            assertEquals(bufferCopy.getPageType(), buff[i].getPageType());
            assertEquals(bufferCopy.getBufferSize(), buff[i].getBufferSize());
        }
    }

    @Test
    public void readOrderIsSequential() throws Exception {

        Exchange ex = _persistit.getExchange("persistit", "WarmupTest", true);
        BufferPool pool = ex.getBufferPool();

        final int full = pool.getBufferCount() * (pool.getBufferSize() / RED_FOX.length());
        /*
         * Overflow the buffer pool
         */
        for (int i = 1; i <= full * 3; i++) {
            ex.getValue().put(RED_FOX);
            ex.clear().append(i).store();
        }
        /*
         * Pull some low-address pages in to scramble the pool
         */
        for (int i = full * 2; i >= 0; i -= 1000) {
            ex.clear().append(i).fetch();
        }
        /*
         * Verify that buffers in pool now have somewhat scrambled page
         * addresses
         */
        int breaks = 0;
        long previous = -1;

        for (int i = 0; i < pool.getBufferCount(); i++) {
            final Buffer b = pool.getBufferCopy(i);
            assertTrue("Every buffer should be valid at this point", b.isValid());
            if (b.getPageAddress() < previous) {
                breaks++;
            }
            previous = b.getPageAddress();
        }

        assertTrue("Buffer pool should have scrambled page address", breaks > 0);

        final Configuration config = _persistit.getConfiguration();
        ex = null;
        pool = null;
        _persistit.close();

        _persistit = new Persistit();
        config.setBufferPreloadEnabled(false);
        _persistit.initialize(config);

        final Volume volume = _persistit.getVolume("persistit");
        final MediatedFileChannel mfc = (MediatedFileChannel) volume.getStorage().getChannel();
        final TrackingFileChannel tfc = new TrackingFileChannel();
        mfc.injectChannelForTests(tfc);
        pool = volume.getStructure().getPool();
        pool.preloadBufferInventory();
        assertTrue("Preload should have loaded pages from journal file", tfc.getReadPositionList().size() > 0);
        tfc.assertSequential(true, true);
    }
}
