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
 * Created on Jun 7, 2004
 */
package com.persistit.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class OO2HTML
extends DefaultHandler
{
    private final static String[] HEAD =
    {
        "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\">",
        "<html>",
        "<head>",
        "  <!-- Copyright (c) 2004 Persistit Corporation.  All rights reserved. -->",
        "  <link type=\"text/css\" href=\"${cssFileName}\" rel=\"stylesheet\">",
        "</head>",
    };
    
    private final static String[] TAIL =
    {
        "</html>",
    };
    
    private final static String CRLF = "\r\n";
    
    private final static String DOC_FILE_SUFFIX = ".sxw";
    private final static String HTML_FILE_SUFFIX = ".html";
    private final static String TEXT_FILE_SUFFIX = ".txt";
    private final static String DOC_ROOT_PATH = "c:\\source\\persistit\\doc\\kit";
    private final static String CSS_FILE_NAME = "persistit.css";
    
    private final static String JAVADOC_PREFIX = "     * ";
    private final static String DOTS = "................................";
    private final static String DTD_URI =
        "file:///C:/Program Files/OpenOffice.org 2.0/share/dtd/officedocument/1_0/";
    private final static String CSSMAP_FILE = "./doc/cssmap.txt";
    private final static String ABOUT_TO_DOC_API_PATH = "../doc/api/";
    private final static String API_DOC_BUILD_PATH = 
        "C:\\Source\\persistit\\build\\javadoc\\";
    private final static String[] USEFUL_ATTRS =
    {
            "font-name",
            "font-weight",
            "font-size",
            "font-style",
            "text-background-color",
            "border",
            "border-left",
            "border-right",
            "border-top",
            "border-bottom",
            "color",
            "background-color",
            "margin",
            "margin-left",
            "margin-right",
            "margin-top",
            "margin-bottom",
            "padding",
            "text-align",
            "vertical-align",
    };
    

    String _cssFileName;
    StringBuffer _sb = new StringBuffer();
    Stack _contextStack = new Stack();
    HashMap _styles = new HashMap();
    Style _style;
    
    boolean _textOnly;
    boolean _forJavadoc;
    boolean _lineDirty;
    boolean _spaceArmed;
    boolean _hyperlink;
    boolean _hyperAbout;
    boolean _makeTOC;
    int _tocLocation = -1;
    
    HashMap _cssMap = new HashMap();
    HashMap _cssMissingMap = new HashMap();
    TreeSet _attrNames = new TreeSet();
    Element _root = null;
    ArrayList _toc = new ArrayList();
    int _tocIndex = 1000;
    boolean _writingTOC;
    //
    // Used in numbering lists in text mode
    // This is just for the license agreement.
    //
    int[] _listItem = new int[10];
    int _listItemDepth = 0;
    
    private static class Element
    {
        String _qName;
        String _tag = "";
        String _tail = "";
        boolean _skip;
        boolean _startLine;
        boolean _endLine;
        boolean _ignoreNoContent;
        boolean _indentInner;
        boolean _noClose;
        HashMap _attrs;
        ArrayList _content;
        int _tocIndex;
        
        private void putAttributes(Attributes attrs)
        {
            for (int index = 0; index < attrs.getLength(); index++)
            {
                String aname = attrs.getQName(index);
                String avalue = attrs.getValue(index);
                if (_attrs == null) _attrs = new HashMap();
                _attrs.put(aname, avalue);
            }
        }
        
        private String getAttributeValue(String name, String dflt)
        {
            if (_attrs == null) return dflt;
            String s = (String)_attrs.get(name);
            if (s == null) return dflt;
            return s;
        }
        
        private String styleName()
        {
            String s = getAttributeValue("text:style-name", "");
            return s;
        }
        
        private boolean isWhiteSpaceOnly()
        {
            if (_content == null) return true;
            for (int index = 0; index < _content.size(); index++)
            {
                Object o = _content.get(index);
                if (o instanceof String)
                {
                    String s = (String)o;
                    for (int i = 0; i < s.length(); i++)
                    {
                        if (s.charAt(i) != ' ') return false;
                    }
                }
                else
                {
                    Element e = (Element)o;
                    if (!e._tag.equals("") || !e.isWhiteSpaceOnly())
                    {
                        return false;
                    }
                }
            }
            return true;
                
        }
        
        public String toString()
        {
            return "Element(qName=" + _qName + " tag=" + _tag + 
                        " tail=" + _tail + " skip=" + _skip +
                        " startLine=" + _startLine + 
                        " endLine=" + _endLine + 
                        " indentInner=" + _indentInner + ")";
        }
    }
    
    private class Style
    {
        private String _name;
        private String _parentName;
        private String _family;
        private String _class;
        private HashMap _attributes;

        private boolean _isResolved;
        private boolean _isNonDefault;
        private boolean _isItalic;
        private boolean _isBold;
        private boolean _isForegroundOnly;
        private boolean _isCodeBlock;
        
        String _cssAttrs;
        private String _cssClassName;
        
        public String toString()
        {
            return "Style(name=" + _name + 
                   ",parent=" + _parentName +
                   ",attrs=" + _attributes + ")";
        }
        
        boolean isBold()
        {
            resolve();
            return _isBold;
        }
        
        public boolean isItalic()
        {
            resolve();
            return _isItalic;
        }
        
        public boolean isNonDefault()
        {
            resolve();
            return _isNonDefault;
        }
        
        public String getCssClassName()
        {
            resolve();
            return _cssClassName;
        }
        
        void resolve()
        {
            if (_isResolved) return;
            _isResolved = true;
            HashMap attrs = _attributes;
            
            Style parent = null;
            StringBuffer sb = new StringBuffer();
            if (_parentName != null)
            {
                parent = getStyle(_parentName);
                _isNonDefault = parent._isNonDefault;
                _isItalic = parent._isItalic;
                _isBold = parent._isBold;
                if (_family == null) _family = parent._family;
            }
            
            if (_family != null)
            {
                sb.append("family=");
                sb.append(_family);
                sb.append(';');
            }
            
            for (int i = 0; i < USEFUL_ATTRS.length; i++)
            {
                _isForegroundOnly = true;
                String aname = USEFUL_ATTRS[i];
                String avalue = (String)attrs.get(aname);
                String pvalue = (parent == null) 
                                ? null
                                : (String)parent._attributes.get(aname);
                if (aname.equals("font-style") && "italic".equals(avalue))
                {
                    _isItalic = true;
                }
                else if (aname.equals("font-weight") && "bold".equals(avalue))
                {
                    _isBold = true;
                }
                else
                {
                    _isNonDefault = true;
                }
                if (avalue == null) avalue = pvalue;
                if (avalue != null)
                {
                    if (sb.length() > 0) sb.append(' ');
                    sb.append(aname);
                    sb.append(':');
                    sb.append(avalue);
                    sb.append(';');
                }
                if (_isNonDefault && !aname.equals("color") && avalue != null) 
                {
                    _isForegroundOnly = false;
                }
            }
            _cssAttrs = sb.toString();
            _cssClassName = (String)_cssMap.get(_cssAttrs);
            if (_cssClassName == null)
            {
                _cssMissingMap.put(_cssAttrs, this);
            }
            else if (_cssClassName.length() == 0) _isNonDefault = false;
            else if ("code-block".equalsIgnoreCase(_cssClassName))
            {
                _isCodeBlock = true;
            }
        }
    }
    
    private OO2HTML(
        String cssFileName,
        boolean textOnly,
        boolean hyper,
        boolean hyperAbout,
        boolean makeTOC)
    throws Exception
    {
        _cssFileName = cssFileName;
        _textOnly = textOnly;
        _hyperlink = hyper;
        _hyperAbout = hyperAbout;
        _makeTOC = makeTOC;
        
        try
        {
            BufferedReader reader = new BufferedReader(
                    new FileReader(CSSMAP_FILE));
            String line = "";
            while ((line = reader.readLine()) != null)
            {
                line = line.trim();
                if (line.length() != 0 &&
                    !line.startsWith("//") &&
                    !line.startsWith("@"))
                {
                    int p = line.indexOf("=");
                    if (p >= 0)
                    {
                        String internalStyles = line.substring(p + 1).trim();
                        String cssName = line.substring(0, p).trim();
                        _cssMap.put(internalStyles, cssName);
                    }
                }
            }
            reader.close();
        }
        catch (IOException ioe)
        {
            System.out.println(ioe);
        }
    }

    public void startElement(
        String uri, 
        String localName, 
        String qName, 
        Attributes attributes)
    throws SAXException
    {
        Element e = new Element();
        e._qName = qName;
        e.putAttributes(attributes);
        _contextStack.push(e);
        return;
    }
    
    public void endElement(String uri, String localName, String qName)
    throws SAXException
    {
        if (_contextStack.isEmpty())
        {
            throw new IllegalStateException("Context stack is empty");
        }
        Element e = (Element)_contextStack.pop();
        if (!qName.equals(e._qName))
        {
            throw new IllegalStateException(
                "endElement(" + qName + ") does not match " +
                "startElement(" + e._qName + ")");
        }
        translateElement(e);
        if (!e._skip)
        {
            Element parent = (Element)_contextStack.peek();
            if (parent._content == null) parent._content = new ArrayList();
            parent._content.add(e);
        }
    }
    
    public void startDocument()
    throws SAXException
    {
        _root = new Element();
        _root._tag = "";
        _contextStack.push(_root);
    }
    
    public void endDocument()
    throws SAXException
    {
        if (_contextStack.size() != 1)
        {
            throw new IllegalStateException(
                "At end of document, Context stack has " + _contextStack.size() +
                " elements");
        }
        _contextStack.pop();
    }
    
    
    public void characters(char[] chars, int start, int length)
    throws SAXException
    {
        StringBuffer sb = new StringBuffer();
        for (int i = start; i < start + length; i++)
        {
            char ch = chars[i];
            String entity = entity(ch);
            if (entity == null) sb.append(ch);
            else sb.append(entity);
        }
        if (_contextStack.isEmpty()) throw new IllegalStateException(
                "Content outside of element: " + sb.toString()); 
        Element e = (Element)_contextStack.peek();
        if (e._content == null) e._content = new ArrayList();
        String s = sb.toString();
        if (_hyperlink)
        {
            apiHyperlink(sb, "com.persistit.", "");
        }
        e._content.add(sb.toString());
    }
    
    public void ignorableWhitespace(char[] ch, int start, int length)
    throws SAXException
    {
        // ignore it!
    }
    
    private String entity(char ch)
    {
//        if (ch == '©') return "&copy;";
//        if (ch == '®') return "&reg;";
//        if (ch == '') return "&trade;";
//        if (ch == '') return "\"";
//        if (ch == '') return "\"";
//        if (ch == '') return "'"; 
//        if (ch == '<') return "&lt;";
//        if (ch == '>') return "&gt;";
//        if (ch == '&') return "&amp;";
//        if (ch == '') return "-"; 
//            
        return null;
    }
    
    private String subst(String s, String r, String w)
    {
        if (s.indexOf(r) >= 0)
        {
            StringBuffer sb = new StringBuffer();
            sb.append(s.substring(0, s.indexOf(r)));
            sb.append(w);
            sb.append(s.substring(s.indexOf(r) + r.length()));
            s = sb.toString();
        }
        return s;
    }
    

    private void writeProlog()
    {
        startNewLine(0);
        for (int i = 0; i < HEAD.length; i++)
        {
            _sb.append(subst(HEAD[i], "${cssFileName}", _cssFileName));
            _lineDirty = true;
            startNewLine(0);
        }
    }                       
                       
    private void writeEpilog()
    {
        startNewLine(0);
        for (int i = 0; i < TAIL.length; i++)
        {
            _sb.append(TAIL[i]);
            _lineDirty = true;
            startNewLine(0);
        }
        startNewLine(0);
    }
    
    private void writeTOC()
    {
        _makeTOC = false;
        _writingTOC = true;
        startNewLine(0);
        int size = _toc.size();
        
        StringBuffer sbSave = _sb;
        _sb = new StringBuffer();

        startNewLine(0);
        _sb.append("<!-- START TOC -->");
        _lineDirty = true;
        
        for (int index = 0; index < size; index++)
        {
            Element e = (Element)_toc.get(index);
            startNewLine(1);
            _sb.append("<");
            _sb.append(e._tag);
            _sb.append(e._tail);
            _sb.append(" class=\"toc\"");
            _sb.append(">");
            _sb.append("<a href=\"#__" + e._tocIndex + "\" class=\"toc\">");
            if (e._content != null && e._content.size() > 0)
            {
                _spaceArmed = false;
                writeContent(e._content, 1, false);
            }
            _sb.append("</a>");
            _sb.append("</");
            _sb.append(e._tag);
            _sb.append(">");
            _lineDirty = true;
        }
        startNewLine(0);
        _sb.append("<br />");
        startNewLine(0);
        _sb.append("<!-- END TOC -->");
        _lineDirty = true;
        startNewLine(0);
        String toc = _sb.toString();
        
        _sb = sbSave;
        if (_tocLocation >= 0)
        {
            _sb.replace(_tocLocation, _tocLocation + 6, toc);
        }
        else
        {
            _sb.append(toc);
        }
        
        _writingTOC = false;
        _lineDirty = true;
        startNewLine(0);
    }
    
    
    public void writeDocument(PrintWriter pw)
    {
        if (!_textOnly) writeProlog();
        coallesceContent(_root);
        writeElement(_root, 0, false);
        if (!_textOnly)
        {
            if (_makeTOC) writeTOC();
            writeEpilog();
        }
        String s = _sb.toString();
        checkCharacters(s);
        pw.println(s);
        pw.flush();
    }
    
    private void checkCharacters(String s)
    {
        String[] lines = s.split(CRLF);
        for (int i = 0; i < lines.length; i++)
        {
            String line = lines[i];
            for (int j = 0; j < line.length(); j++)
            {
                char c = line.charAt(j);
                if (c < 0x20 || c > 0x7E)
                {
                    System.out.println("Bad char line " + i + ": " + line);
                    break;
                }
            }
        }
    }
    
    private void writeElement(Element e, int level, boolean pre)
    {
        if (e._tag.length() == 0)
        {
            writeContent(e._content, level, pre);
        }
        else if (_textOnly)
        {
            writeElementTextMode(e, level, pre);
        }
        else
        {
            if (!pre)
            {
                if (e._startLine) startNewLine(level);
                else if (_spaceArmed)
                {
                    _spaceArmed = !wrap(e._tag.length() + e._tail.length() + 3, level);
                }
                if (_spaceArmed) _sb.append(' ');
                _spaceArmed = false;
            }
        
            boolean tocWrapper = false;
            if (_makeTOC && !_writingTOC && isHeaderTag(e._tag))
            {
                tocWrapper = true;
                e._tocIndex = ++_tocIndex;
                _sb.append("<a name=\"__" + (e._tocIndex) + "\">");
                _toc.add(e);
            }
            if (!_writingTOC || !(e._tag.equals("a")))
            {
                _sb.append("<");
                _sb.append(e._tag);
                _sb.append(e._tail);
                _lineDirty = true;
                if (e._content != null && e._content.size() > 0)
                {
                    _sb.append(">");
                    if (e._tag.equals("pre"))
                    {
                        writeContent(e._content, 0, true);
                    }
                    else
                    {
                        writeContent(
                            e._content, 
                            e._indentInner ? level + 1 : level, 
                            pre);
                    }
                    if (!pre)
                    {
                        if (e._endLine) startNewLine(level);
                        else if (_spaceArmed)
                        {
                            _spaceArmed = !wrap(e._tag.length() + e._tail.length() + 3, level);
                        }
                        if (_spaceArmed) _sb.append(' ');
                        _spaceArmed = false;
                        //else wrap(e._tag.length() + 3, level);
                    }
                    _sb.append("</");
                    _sb.append(e._tag);
                    _sb.append(">");
                }
                else
                {
                    _sb.append(" />");
                }
                if (tocWrapper)
                {
                    _sb.append("</a>");
                }
                _lineDirty = true;
            }
        }
    }
    
    private boolean isHeaderTag(String s)
    {
        return
            s.length() == 2 && 
            (s.charAt(0) == 'h' || s.charAt(0) == 'H') &&
            (Character.isDefined(s.charAt(1)));
    }
    
    private void writeElementTextMode(Element e, int level, boolean pre)
    {
        {
            if (!pre)
            {
                if (e._startLine)
                {
                    if (e._tag.equals("li") && _listItemDepth > 0)
                    {
                        String s = "";
                        _listItem[_listItemDepth - 1]++;
                        for (level = 0; level < _listItemDepth; level++)
                        {
                            if (level > 1) s += ".";
                            s += Integer.toString(_listItem[level]);
                            if (level == 0) s += ".";
                        }
                        startNewLine(level - 1);
                        _sb.append(s);
                        if (s.length() < 5) _sb.append("     ".substring(s.length()));
                        _lineDirty = true;
                    }
                    else
                    {
                        _spaceArmed = false;
                    }
                }
                else if (_spaceArmed)
                {
                    _sb.append(' ');
                }
                _spaceArmed = false;
            }
        
            if (e._content != null && e._content.size() > 0)
            {
                if (e._tag.equals("pre"))
                {
                    writeContent(e._content, 0, true);
                }
                else
                {
                    if (e._tag.equals("ol"))
                    {
                        _listItemDepth++;
                        _listItem[_listItemDepth] = 0;
                    }
                    writeContent(
                        e._content, 
                        e._indentInner ? level + 1 : level, 
                        pre);
                    if (e._tag.equals("ol"))
                    {
                        _listItemDepth--;
                    }
                }
                if (!pre)
                {
                    if (_lineDirty && (e._startLine || e._endLine))
                    {
                        startNewLine(0);
                        _lineDirty = true;
                        startNewLine(0);
                    }
                }
            }
        }
    }
    
    private void writeContent(ArrayList content, int level, boolean pre)
    {
        if (content == null || content.size() == 0) return;
        for (int i = 0; i < content.size(); i++)
        {
            Object o = content.get(i);
            if (o instanceof String)
            {
                String s = (String)o;
                if (pre)
                {
                    writeString(s);
                    _lineDirty = true;
                }
                else
                {
                    int q = 0;
                    for (int p = 0; p < s.length(); p++)
                    {
                        int ch = s.charAt(p);
                        if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n')
                        {
                            if (p - q > 0)
                            {
                                if (_spaceArmed)
                                {
                                    _spaceArmed = !wrap(p - q, level);
                                }
                                if (_spaceArmed)
                                {
                                    _sb.append(' ');
                                }
                                writeString(s.substring(q, p));
                                _lineDirty = true;
                            }
                            q = p + 1;
                            _spaceArmed = true;
                        }
                    }
                    if (q < s.length())
                    {
                        if (_spaceArmed)
                        {
                            wrap(s.length() - q, level);
                        }
                        if (_spaceArmed)
                        {
                            _sb.append(' ');
                        }
                        writeString(s.substring(q));
                        _spaceArmed = false;
                        _lineDirty = true;
                    }
                }
            }
            else
            {
                Element e = (Element)o;
                if (e._tag.length() == 0)
                {
                    if (e._content != null)
                    {
                        writeContent(e._content, level, pre);
                    }
                }
                else
                {
                    writeElement(e, level, pre);
                }
            }
        }
    }
    
    private void writeString(String s)
    {
        int p = s.indexOf("${TOC}");
        if (_makeTOC && p >= 0 && _tocLocation < 0)
        {
            _tocLocation = _sb.length() + p;
        }
        _sb.append(s);
    }
    
    private void apiHyperlink(StringBuffer sb, String prefix, String path)
    {
        int p = -1;
        for (;;)
        {
            p = sb.indexOf(prefix, p);
            if (p < 0) return;
            int r = 0;
            if (p > 0 && Character.isJavaIdentifierPart(sb.charAt(p - 1)))
            {
                p++;
            }
            else
            {
                boolean isCapitalized = false;
                boolean isDot = false;
                for (int q = p + prefix.length() - 1;; q++)
                {
                    int ch = q < sb.length() ? sb.charAt(q) : -1;
                    if (!Character.isJavaIdentifierPart((char)ch) && 
                        ch != '.')
                    {
                        if (r == 0 || !isCapitalized)
                        {
                            p++;
                            break;
                        }
                        String fqcn = sb.substring(p, r + 1);
                        if (!fqcn.startsWith("com.persistit.ui."))
                        {
                            fqcn = fqcn.replace('.', '/').replace('$', '.');
                            File apiFile = new File(API_DOC_BUILD_PATH, fqcn + ".html");
                            if (apiFile.exists())
                            {
                                sb.insert(r + 1, "</a>");
                                String a = "<a href=\"" + 
                                           (_hyperAbout ? ABOUT_TO_DOC_API_PATH : "") +
                                           fqcn + ".html\">";
                                sb.insert(p, a);
                                p += 4 + a.length() + prefix.length();
                            }
                            else
                            {
                                p += prefix.length();
                            }
                        }
                        else
                        {
                            p += prefix.length();
                        }
                        break;
                    }
                    r = q;
                    if (ch == '.') isDot = true;
                    else if (isDot)
                    {
                        isDot = false;
                        isCapitalized = (ch >= 'A' && ch <= 'Z');
                    }
                }
            }
        }
    }

    private boolean wrap(int t, int level)
    {
        int length = _sb.length() - _sb.lastIndexOf(CRLF);
        if (length > 30 && 
                (_forJavadoc && length + t > 65 || length + t > 72))
        {
            startNewLine(level);
            return true;
        }
        return false;
    }
    
    private void startNewLine(int level)
    {
        if (_lineDirty)
        {
            _sb.append(CRLF);
            _spaceArmed = false;
        }
        if (_forJavadoc)
        {
            _sb.append(JAVADOC_PREFIX);
        }
        for (int i = 0; i < level; i++)
        {
            _sb.append(_textOnly ? "     " :"  ");
        }
        _lineDirty = false;
    }
    
    private Style getStyle(String name)
    {
        Style style = (Style)_styles.get(name);
        if (style != null) style.resolve();
        return style;
    }
    
    private void translateElement(Element e)
    {
        if ("text:h".equals(e._qName))
        {
            String n = e.getAttributeValue("text:level", "");
            e._tag = "h" + n;
//            e._tail = " class=\"persistit\"";
            e._startLine = true;
            e._indentInner = true;
            e._endLine = false;
            e._ignoreNoContent = true;
        }
        else if ("text:p".equals(e._qName))
        {
            Style style = getStyle(e.styleName());
            e._tag = "p";
            if (style != null)
            {
                style.resolve();
                if (style._isCodeBlock)
                {
                    e._tag="pre";
//                    e._tail = " class=\"persistit\"";
                }
                else if (style.isNonDefault())
                {
                    e._tail += " class=\"" + style.getCssClassName() + "\"";
                }
            }
            e._startLine = true;
            e._endLine = false;
            if (!_textOnly)
            {
                e._endLine = true;
                e._indentInner = true;
            }
            coallesceContent(e);
            e._ignoreNoContent = true;
        }
        else if ("text:span".equals(e._qName))
        {
            Style style = getStyle(e.styleName());
            if (style == null)
            {
                System.out.println("missing style " + e.styleName());
            }
            else if (style.isNonDefault())
            {
                e._tag = "span";
                String cssClassName = style.getCssClassName();
                if (style._isForegroundOnly && e.isWhiteSpaceOnly())
                {
                    // No need to say a space is "black" or "bold"
                    e._tag = "";
                }
                else e._tail += " class=\"" + style.getCssClassName() + "\"";
            }
            else
            {
                if (style.isItalic())
                {
                    e._tag = "i";
                }
                else if (style.isBold())
                {
                    e._tag = "b";
                }
                else
                {
                    e._tag = "";
                }
            }
            e._ignoreNoContent = true;
        }
        else if ("text:a".equals(e._qName))
        {
            String href = e.getAttributeValue("xlink:href", "#");
            e._tag = "a";
            if (href.startsWith("^^^"))
            {
                href = href.substring(3);
            }
            e._tail += " href=\"" + href + "\"";
        }
        else if ("text:bookmark".equals(e._qName) ||
                 "text:bookmark-start".equals(e._qName))
        {
            String href = e.getAttributeValue("text:name", "#");
            e._tag = "a";
            e._tail = " name=\"" + href + "\"";
        }
        else if ("text:bookmark-end".equals(e._qName))
        {
            e._skip = true;
        }
        else if ("text:s".equals(e._qName))
        {
            e._tag = "";
            String s = e.getAttributeValue("text:c", "1");
            int count = Integer.parseInt(s);
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < count; i++)
            {
                sb.append(' ');
            }
            if (e._content == null) e._content = new ArrayList();
            e._content.add(sb.toString());
        }
        
        else if ("text:ordered-list".equals(e._qName))
        {
            e._tag = "ol";
//            e._tail = " class=\"persistit\"";
            e._indentInner = true;
            e._startLine = true;
            e._endLine = true;
            String style = e.styleName();
            if (!"".equals(style)) e._tail += " class=\"" + style + "\"";
            e._ignoreNoContent = true;
        }
        else if ("text:unordered-list".equals(e._qName))
        {
            e._tag = "ul";
//            e._tail = " class=\"persistit\"";
            e._startLine = true;
            e._endLine = true;
            e._indentInner = true;
            String style = e.styleName();
            if (!"".equals(style)) e._tail += " class=\"" + style + "\"";
            e._ignoreNoContent = true;
        }
        else if ("text:list-item".equals(e._qName))
        {
            e._tag = "li";
//            e._tail = " class=\"persistit\"";
            e._startLine = true;
            e._endLine = true;
            e._indentInner = false;
            e._ignoreNoContent = true;
        }
        else if ("text:line-break".equals(e._qName))
        {
            e._tag="br";
            e._startLine = true;
        }
        
        else if ("table:table".equals(e._qName))
        {
            e._tag = "table";
//            e._tail = " class=\"persistit\"";
            e._startLine = true;
            e._endLine = true;
            e._indentInner = true;
            e._ignoreNoContent = true;
        }
        else if ("table:table-row".equals(e._qName))
        {
            e._tag = "tr";
//            e._tail = " class=\"persistit\"";
            e._startLine = true;
            e._indentInner = true;
            e._endLine = true;
        }
        else if ("table:table-cell".equals(e._qName))
        {
            e._tag = "td";
//            e._tail = " class=\"persistit\"";
            e._startLine = true;
            e._indentInner = true;
            e._endLine = true;
        }
        else if ("office:body".equals(e._qName))
        {
            e._tag = "body";
            e._tail = " class=\"persistit\"";
            e._startLine = true;
            e._endLine = true;
            coallesceContent(e);
            e._ignoreNoContent = true;
        }
        else if ("office:document-content".equals(e._qName))
        {
            e._tag = "";
        }
        else if ("style:default-style".equals(e._qName) ||
                 "style:style".equals(e._qName))
        {
            Style style = new Style();
            style._name = e.getAttributeValue("style:name", null);
            style._parentName = e.getAttributeValue("style:parent-style-name", null);
            style._family = e.getAttributeValue("style:family", null);
            style._class = e.getAttributeValue("style:class", null);
            style._attributes = new HashMap();
            if (e._content != null)
            {
                for (int i = 0; i < e._content.size(); i++)
                {
                    Object o = e._content.get(i);
                    if (o instanceof Element)
                    {
                        style._attributes.putAll(((Element)o)._attrs);
                    }
                }
            }
            _styles.put(style._name, style);
            e._ignoreNoContent = true;
        }
        else if ("style:properties".equals(e._qName))
        {
            HashMap usefulAttributes = new HashMap();
            for (Iterator iter = e._attrs.entrySet().iterator();
                 iter.hasNext();)
            {
                Map.Entry entry = (Map.Entry)iter.next();
                String attrName = (String)entry.getKey();
                int k = attrName.indexOf(':');
                if (k >= 0) attrName = attrName.substring(k + 1);
                _attrNames.add(attrName);
                boolean useful = false;
                for (int j = 0; j < USEFUL_ATTRS.length; j++)
                {
                    if (USEFUL_ATTRS[j].equals(attrName))
                    {
                        useful = true;
                        break;
                    }
                }
                if (useful)
                {
                    String attrValue = (String)entry.getValue();
                    usefulAttributes.put(attrName, attrValue);
                }
            }
            e._attrs = usefulAttributes;
            e._ignoreNoContent = true;
        }
        else if ("text:sequence-decls".equals(e._qName) ||
                 "text:tab-stop".equals(e._qName) ||
                 "table:sequence-decls".equals(e._qName) ||
                 "table:table-column".equals(e._qName) ||
                 "table:table-header-rows".equals(e._qName) ||
                 "office:script".equals(e._qName) ||
                 "office:forms".equals(e._qName) ||
                 "style:tab-stops".equals(e._qName) ||
                 "style:backgroup-image".equals(e._qName) ||
                 "style:master-styles".equals(e._qName) ||
                 "style:master-page".equals(e._qName) ||
                 "office:font-decls".equals(e._qName) ||
                 "office:styles".equals(e._qName) ||
                 "office:office:document-styles".equals(e._qName) ||
                 "office:document-styles".equals(e._qName) ||
                 "office:automatic-styles".equals(e._qName))
        {
            e._ignoreNoContent = true;
            e._skip = true;;
        }
        else
        {
            e._tag = "???" + e._qName;
        }
        if (e._content != null) e._content.trimToSize();
    }
    
    /**
     * Massage the content of this element.
     * @param e
     */
    public void coallesceContent(Element parent)
    {
        if (parent._content == null || parent._content.size() <= 1) return;
        ArrayList list = parent._content;
        int columns = 0;
        for (int index = 0; index < parent._content.size(); index++)
        {
            Object o = list.get(index);
            if (o instanceof Element)
            {
                Element e = (Element)o;
                coallesceContent(e);
                
                if (e._skip || (e._ignoreNoContent && e._content == null))
                {
                    list.remove(index);
                    index--;
                }
                else if (e._tag.equals("pre"))
                {
                    if (e._content == null)
                    {
                        throw new IllegalStateException(
                                "<pre> element " + e + " with no content");
                    }
                    if (index + 1 < list.size() &&
                        (list.get(index + 1) instanceof Element))
                    {
                        Element e2 = (Element)list.get(index + 1);
                        if (e2._tag.equals("pre"))
                        {
                            e._content.add("\r\n");
                            if (e2._content != null)
                            {
                                e._content.addAll(e2._content);
                            }
                            list.remove(index + 1);
                            index--;
                        }
                    }
                    else
                    {
                        System.out.print("");
                    }
                }
                if ("table".equals(parent._tag) &&
                    "tr".equals(e._tag))
                {
                    if (e._content != null && e._content.size() > columns)
                    {
                        columns = e._content.size();
                    }
                }
            }
        }
        if (columns > 0)
        {
            for (int i = 0; i < columns; i++)
            {
                Element e = new Element();
                e._tag = "col";
                e._tail = " class=\"col" + i + "\"";
                list.add(i, e);
            }
        }
        list.trimToSize();
    }

    public static void main(String[] args)
    throws Exception
    {
        boolean missing = false;
        boolean all = false;
        boolean text = false;
        boolean hyper = false;
        boolean hyperAbout = false;
        boolean makeTOC = false;
        
        int argi = 0;
        String arg = args[argi];
        if (arg.startsWith("-"))
        {
            argi++;
            missing = arg.indexOf('m') >= 0;
            all = arg.indexOf('a') >= 0;
            text = arg.indexOf('t') >= 0;
            hyper = arg.indexOf('h') >= 0;
            hyperAbout = arg.indexOf('H') >= 0;
            makeTOC = arg.indexOf('c') >= 0;
        }
        
        String fromPath = args[argi++];
        
        if (!fromPath.toLowerCase().endsWith(DOC_FILE_SUFFIX))
        {
            fromPath += DOC_FILE_SUFFIX;
        }
        
        File fromFile = new File(fromPath);
        fromPath = fromFile.getAbsolutePath();
        
        String toPath = 
            fromPath.substring(0, fromPath.length() - 4) +
            (text ? TEXT_FILE_SUFFIX : HTML_FILE_SUFFIX);

        String cssFileName = CSS_FILE_NAME;
        String s = fromPath;
        if (s.toLowerCase().startsWith(DOC_ROOT_PATH))
        {
            s = s.substring(DOC_ROOT_PATH.length());
            int p = 0;
            while ((p = s.indexOf('\\', p + 1)) != -1)
            {
                cssFileName = "../" + cssFileName;
            }
                    
        }
        
        System.out.println("fromFile:  " + fromFile.getAbsolutePath());
        System.out.println("toPath:    " + toPath);
        System.out.println("cssPath:   " + cssFileName);
        
        ZipFile zf = new ZipFile(fromFile);
        ZipEntry ze = zf.getEntry("styles.xml");
        InputStream is = zf.getInputStream(ze);
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser parser = factory.newSAXParser();

        OO2HTML handler = new OO2HTML(cssFileName, text, hyper, hyperAbout, makeTOC);
        parser.parse(is, handler, DTD_URI);

        ze = zf.getEntry("content.xml");
        is = zf.getInputStream(ze);
        factory = SAXParserFactory.newInstance();
        parser = factory.newSAXParser();
        parser.parse(is, handler, DTD_URI);
        
        if (handler._cssMissingMap.isEmpty())
        {
            PrintWriter pw = new PrintWriter(new FileOutputStream(toPath, false));
            handler.writeDocument(pw);
        }
        
        is.close();

        int count = handler._cssMissingMap.size();
        System.out.println();
        System.out.println(count + " unmapped styles");
        System.out.println();
        
        PrintWriter writer = null;
        for (Iterator iter = handler._cssMissingMap.entrySet().iterator();
             iter.hasNext(); )
        {
            Map.Entry entry = (Map.Entry)iter.next();
            String key = (String)entry.getKey();
            Style style = (Style)entry.getValue();
            StringBuffer sb = new StringBuffer();
            sb.append("@");
            sb.append(style._name);
            for (int z = style._name.length(); z < 16; z++)
            {
                sb.append(" ");
            }
            sb.append("= ");
            sb.append(key);
            
            if (missing)
            {
                if (writer == null)
                {
                    writer =
                        new PrintWriter(
                            new BufferedWriter(
                                new FileWriter(CSSMAP_FILE, true)));
                    
                    writer.println();
                    writer.println("//");
                    writer.println("// from " + fromFile);
                    writer.println("//");
                    writer.println();
                }
                writer.println(sb.toString());
            }
            System.out.println(sb.toString());
        }
        if (writer != null)
        {
            writer.println();
            writer.close();
        }
        System.out.println();
        
        if (all)
        {
            System.out.println();
            System.out.println(handler._attrNames.size() + " attribute names");
            System.out.println();
            for (Iterator iter = handler._attrNames.iterator();
                 iter.hasNext(); )
            {
                System.out.println(iter.next());
            }
        }
    }
}
