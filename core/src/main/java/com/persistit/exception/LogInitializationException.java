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
 * This is a wrapper for any {@link Exception} that occurs while initializing
 * the logging subsystem. It is frequently convenient for the caller of a
 * Persistit method to catch {@link PersistitException}s without also needing to
 * catch <tt>IOException</tt>s or other <tt>Exception</tt> subclasses. For
 * compatibility with earlier J2SE releases this is implemented without using
 * JDK 1.4 Exception chaining.
 * 
 * @version 1.0
 */
public class LogInitializationException extends PersistitException {
    private static final long serialVersionUID = -3253500224779009799L;

    public LogInitializationException(Exception e) {
        super(e);
    }
}
