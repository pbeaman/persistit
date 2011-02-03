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
 * Thrown by {@link com.persistit.Volume} on an attempt to create a Volume with
 * invalid buffer size, initial size, extension size or maximum size..
 * 
 * @version 1.0
 */
public class InvalidVolumeSpecificationException extends PersistitException {
    private static final long serialVersionUID = 5310678046457279454L;

    public InvalidVolumeSpecificationException() {
        super();
    }

    public InvalidVolumeSpecificationException(String msg) {
        super(msg);
    }

}
