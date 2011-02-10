/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit.exception;

import com.persistit.Debug;

/**
 * Thrown by {@link com.persistit.Volume} if the volume file being opened is
 * corrupt. Generally it will be necessary to restore a backup copy of the
 * volume file to resolve this.
 * 
 * @version 1.0
 */
public class CorruptVolumeException extends PersistitException {
    private static final long serialVersionUID = 9119544306031815864L;

    public CorruptVolumeException() {
        super();
    }

    public CorruptVolumeException(String msg) {
        super(msg);
        if (Debug.ENABLED)
            Debug.debug1(true);
    }

}
