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

import java.io.File;

import com.persistit.exception.InUseException;
import com.persistit.exception.PersistitException;

/**
 * Manage all details of file I/O for a special <code>Volume</code> that backs
 * the {@link Exchange#lock()} method. The lock volume differs from a temporary
 * volume in the following ways:
 * <ul>
 * <li>It has a defined name and is visible in the
 * {@link Persistit#getVolumes()} array.</li>
 * <li>It is threadsafe.</li>
 * </ul>
 * Like a temporary volume, it is intended that pages of the lock volume should
 * seldom, if ever, need to be written to disk. And also like a temporary
 * volume, the contents of the lock volume are not recovered during startup.
 * 
 * @author peter
 */
class VolumeStorageL2 extends VolumeStorageT2 {

    final static String LOCK_VOLUME_FILE_PREFIX = "persistit_lockvol_";
    private Buffer _headBuffer;

    VolumeStorageL2(final Persistit persistit, final Volume volume, final File tempDirectory) {
        super(persistit, volume, tempDirectory);
    }

    /**
     * Indicate whether this is a temporary volume. The Lock volume is not
     * temporary, although it inherits most of the characteristics of a
     * temporary volume.
     * 
     * @return <code>true</code> if this volume is temporary
     */
    @Override
    boolean isTemp() {
        return false;
    }

    @Override
    void truncate() throws PersistitException {
        if (!claim(true, 0)) {
            throw new InUseException("Unable to acquire claim on " + this);
        }
        try {
            _headBuffer = _volume.getStructure().getPool().get(_volume, 0, true, false);
            _headBuffer.init(Buffer.PAGE_TYPE_HEAD);
            _headBuffer.setFixed();

            truncateInternal();
            releaseHeadBuffer();

        } finally {
            release();
        }
    }

    @Override
    void claimHeadBuffer() throws PersistitException {
        if (!_headBuffer.claim(true)) {
            throw new InUseException("Unable to acquire claim on " + _headBuffer);
        }
    }

    @Override
    void releaseHeadBuffer() {
        _headBuffer.release();
    }

}
