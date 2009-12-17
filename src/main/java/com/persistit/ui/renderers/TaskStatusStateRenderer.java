/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Apr 5, 2005
 */
package com.persistit.ui.renderers;
import com.persistit.ui.AdminUI;
import com.persistit.ui.ManagementTableModel.AbstractCustomTableCellRenderer;
/**
 * @author Peter Beaman
 * @version 1.0
 */
public class TaskStatusStateRenderer
extends AbstractCustomTableCellRenderer
{
    private AdminUI _adminUI;
    
    public void setup(AdminUI ui, Class baseClass, String columnSpec)
    {
        _adminUI = ui;
    }

    public void setValue(Object value)
    {
        String text = "?";
        if (value instanceof Integer)
        {
            text = _adminUI.getTaskStateString(((Integer)value).intValue());
        }
        setText(text);
    }
    
}
