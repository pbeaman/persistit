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

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

/**
 * Persistit rev: 284
 * 
 * The volume header has a field to store the creation time. This isn't used for
 * anything directly but is displays through various interfaces (management,
 * JMX). Other than being a nice attribute for reference, having the value be
 * real would make for a nice sanity check when debugging various issues (could
 * compare to journal creation times, etc).
 * 
 * The creation time is saved in the statistics structure upon first creation
 * (VolumeStorageT2()#create() -> #truncate()), but the value isn't copied into
 * the header area after that (#flushMetaData() -> updateMetaData()).
 */
public class Bug989202Test extends PersistitUnitTestCase {

    @Test
    public void testVolumeCreateTime() throws Exception {
        final Volume volume = _persistit.getVolume("persistit");
        final long createTime = volume.getStatistics().getCreateTime();
        volume.close();
        final Volume reopened = _persistit.loadVolume(volume.getSpecification());
        final long createTimeReopened = reopened.getStatistics().getCreateTime();
        assertEquals("Create time should be preserved when a volume is closed", createTime, createTimeReopened);

    }

}
