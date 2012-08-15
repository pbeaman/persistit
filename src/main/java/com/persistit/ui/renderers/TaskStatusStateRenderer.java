/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
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
    public void setup(final AdminUI ui, final Class baseClass, final String columnSpec) {
        _adminUI = ui;
    }

    @Override
    public void setValue(final Object value) {
        String text = "?";
        if (value instanceof Integer) {
            text = _adminUI.getTaskStateString(((Integer) value).intValue());
        }
        setText(text);
    }

}
