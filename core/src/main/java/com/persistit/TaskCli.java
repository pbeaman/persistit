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

package com.persistit;

/**
 * Shim for AdminUI to invoke commands via the ManagementCommand interface.
 * @author peter
 *
 */
public class TaskCli extends Task {

    private String _status = "not started";
    private String _commandLine;
    
    @Override
    protected void setupArgs(String[] args) throws Exception {
        _commandLine = args[0];
    }

    @Override
    public void setupTaskWithArgParser(final String[] args) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void runTask() throws Exception {
        _status = _persistit.getManagement().launch(_commandLine);
    }

    @Override
    public String getStatus() {
        return _status;
    }

}
