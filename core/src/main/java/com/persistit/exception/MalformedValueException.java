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
 * Thrown by decoding methods of the {@link com.persistit.Value} class when the
 * serialized byte array is corrupt. This is a catastrophic failure that
 * signifies external volume file corruption.
 * 
 * @version 1.0
 */
public class MalformedValueException extends RuntimeException {
    private static final long serialVersionUID = 5868710861424952291L;

    public MalformedValueException() {
        super();
    }

    public MalformedValueException(String msg) {
        super(msg);
    }
}
