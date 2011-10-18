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

package com.persistit.ui.renderers;

import com.persistit.ui.AdminUI;
import com.persistit.ui.ManagementTableModel.AbstractCustomTableCellRenderer;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class TaskStatusStateRenderer extends AbstractCustomTableCellRenderer {
    private AdminUI _adminUI;

    @Override
    public void setup(AdminUI ui, Class baseClass, String columnSpec) {
        _adminUI = ui;
    }

    @Override
    public void setValue(Object value) {
        String text = "?";
        if (value instanceof Integer) {
            text = _adminUI.getTaskStateString(((Integer) value).intValue());
        }
        setText(text);
    }

}
