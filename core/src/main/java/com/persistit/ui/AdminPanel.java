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

package com.persistit.ui;

import java.rmi.RemoteException;
import java.util.Map;

import javax.swing.JPanel;

public abstract class AdminPanel extends JPanel {

    protected abstract void setup(AdminUI ui) throws NoSuchMethodException,
            RemoteException;

    protected abstract void refresh(boolean reset) throws RemoteException;

    protected abstract Map getMenuMap();

    protected abstract void setDefaultButton();

    protected void setIsShowing(boolean isShowing) {
    }
}
