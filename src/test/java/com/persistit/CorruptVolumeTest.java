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

import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.PersistitException;

public class CorruptVolumeTest extends PersistitUnitTestCase {

    private final String _volumeName = "persistit";

    @Test
    public void testCorruptVolume() throws PersistitException {
        final Exchange exchange = _persistit.getExchange(_volumeName, "CorruptVolumeTest", true);
        // store some records
        exchange.getValue().put("CorruptVolumeTest");
        for (int i = 0; i < 10000; i++) {
            exchange.to(i).store();
        }
        // Corrupt the volume by zonking the the index page
        final Buffer buffer = exchange.getBufferPool().get(exchange.getVolume(), 4, true, true);
        Arrays.fill(buffer.getBytes(), 20, 200, (byte) 0);
        buffer.setDirtyAtTimestamp(_persistit.getTimestampAllocator().updateTimestamp());
        buffer.releaseTouched();
        //
        try {
            exchange.to(9000).fetch();
            fail("Should have gotten a CorruptVolumeException");
        } catch (final CorruptVolumeException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

}
