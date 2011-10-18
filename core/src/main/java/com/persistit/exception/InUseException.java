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
 * Thrown when a Persistit database operation fails to establish a claim on a
 * resource within a reasonable length of time. The timeout is a property of the
 * {@link com.persistit.Exchange} that initiated the failed operation.
 * 
 * @version 1.0
 */
public class InUseException extends PersistitException {
    private static final long serialVersionUID = -4898002482348605103L;

    public InUseException(String msg) {
        super(msg);
    }
}
