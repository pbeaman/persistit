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

/**
 * Thrown when an application attempts to perform operations on a
 * {@link com.persistit.Volume} that has been closed.
 * 
 * @version 1.0
 */
public class VolumeClosedException extends PersistitException {
    private static final long serialVersionUID = -1577835912746168786L;

    public VolumeClosedException() {
        super();
    }

    public VolumeClosedException(String msg) {
        super(msg);
    }
}