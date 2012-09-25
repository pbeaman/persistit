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

import static org.junit.Assert.*;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.UnitTestProperties;

public class VolumeStructureTest extends PersistitUnitTestCase {

    private Exchange exchange() throws PersistitException {
        return _persistit.getExchange(UnitTestProperties.VOLUME_NAME, "VolumeStructureTest", true);
    }

    @Test
    public void pagesAreDeallocated() throws Exception {
        final Exchange ex = exchange();
        ex.getValue().put(RED_FOX);
        long nextAvailablePage = -1;
        for (int j = 0; j < 10; j++) {
            for (int i = 1; i < 10000; i++) {
                ex.to(i).store();
            }
            if (j == 0) {
                nextAvailablePage = ex.getVolume().getStorage().getNextAvailablePage();
            } else {
                assertEquals("removeAll should deallocate all pages", nextAvailablePage, ex.getVolume().getStorage()
                        .getNextAvailablePage());
            }
            for (int i = 1; i < 10000; i++) {
                ex.to(i).remove();
            }
        }
    }
}
