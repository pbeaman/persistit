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
 * Thrown when an operation has waited too long for an operation to complete.
 * The {@link com.persistit.Exchange#setTimeout} method adjusts the timeout
 * value.
 * 
 * @version 1.0
 */
public class TimeoutException extends PersistitException {
    private static final long serialVersionUID = 4021219607905771380L;

    public TimeoutException() {
        super();
    }

    public TimeoutException(String msg) {
        super(msg);
    }
}