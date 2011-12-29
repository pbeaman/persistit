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
 * The superclass for all checked Persistit&trade; Exceptions. This class can
 * also serve as a wrapper for other Exception types.
 * 
 * @version 1.0
 */
public class MissingThreadException extends PersistitException {

    private static final long serialVersionUID = -8988155339592216974L;

    protected MissingThreadException() {
        super();
    }

    public MissingThreadException(String msg) {
        super(msg);
    }

}
