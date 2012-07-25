/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;


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
        long createTime = volume.getStatistics().getCreateTime();
        volume.close();
        Volume reopened = _persistit.loadVolume(volume.getSpecification());
        long createTimeReopened = reopened.getStatistics().getCreateTime();
        assertEquals("Create time should be preserved when a volume is closed", createTime, createTimeReopened);

    }

}
