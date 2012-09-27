/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;

public class VolumeStructureTest extends PersistitUnitTestCase {

    private Exchange exchange() throws PersistitException {
        return _persistit.getExchange(UnitTestProperties.VOLUME_NAME, "VolumeStructureTest", true);
    }

    private long nextAvailable() {
        return _persistit.getVolume(UnitTestProperties.VOLUME_NAME).getNextAvailablePage();
    }

    @Test
    public void pagesAreActuallyDeallocated() throws Exception {
        final Exchange ex = exchange();
        ex.getValue().put(RED_FOX);
        long nextAvailablePage = -1;
        for (int j = 0; j < 10; j++) {
            for (int i = 1; i < 10000; i++) {
                ex.to(i).store();
            }
            if (j == 0) {
                nextAvailablePage = nextAvailable();
            } else {
                assertEquals("removeAll should deallocate all pages", nextAvailablePage, ex.getVolume().getStorage()
                        .getNextAvailablePage());
            }
            for (int i = 1; i < 10000; i++) {
                ex.to(i).remove();
            }
        }
    }

    @Test
    public void harvestLongOnFullGarbagePage() throws Exception {
        final Exchange ex = exchange();
        ex.getValue().put(createString(1000000));
        ex.to(250).append(Key.BEFORE);
        for (int k = 0; k < 10; k++) {
            ex.to(k).store();
        }
        ex.clear();
        final long firstAvailable = nextAvailable();
        final long until = firstAvailable + nextAvailable() / Buffer.GARBAGE_BLOCK_SIZE;
        ex.getValue().put(RED_FOX);
        int count = 0;
        for (count = 0; nextAvailable() < until; count++) {
            ex.to(count).store();
        }
        ex.removeAll();

        _persistit.checkAllVolumes();
    }

    @Test
    public void harvestLong() throws Exception {
        final Exchange ex = exchange();
        long firstAvailable = -1;
        for (int j = 0; j < 10; j++) {

            ex.getValue().put(createString(100));
            for (int i = 0; i < 5000; i++) {
                ex.to(i).store();
            }
            ex.getValue().put(createString(1000000));
            for (int i = 200; i < 300; i++) {
                if ((i % 10) == 0) {
                    ex.to(i).store();
                }
            }
            ex.clear();
            if (j == 0) {
                firstAvailable = nextAvailable();
            } else {
                System.out.printf("%,d -- %,d\n", firstAvailable, nextAvailable());
                assertEquals("Lost some pages", firstAvailable, nextAvailable());
            }
            ex.removeAll();
        }
        _persistit.checkAllVolumes();
    }
}
