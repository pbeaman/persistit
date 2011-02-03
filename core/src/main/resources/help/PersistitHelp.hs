<?xml version='1.0' encoding='ISO-8859-1' ?>
<!DOCTYPE helpset
  PUBLIC "-//Sun Microsystems Inc.//DTD JavaHelp HelpSet Version 2.0//EN"
         "../dtd/helpset_2_0.dtd">

<helpset version="1.0">

  <!-- title -->
  <title>Persistit AdminUI Help</title>

  <!-- maps -->
  <maps>
     <homeID>top</homeID>
     <mapref location="Map.jhm"/>
  </maps>

  <!-- views -->
  <view>
    <name>TOC</name>
    <label>Table Of Contents</label>
    <type>javax.help.TOCView</type>
    <data>PersistitHelpTOC.xml</data>
  </view>

  <view>
    <name>Index</name>
    <label>Index</label>
    <type>javax.help.IndexView</type>
    <data>PersistitHelpIndex.xml</data>
  </view>

  <presentation default="true" displayviewimages="false">
    <name>main window</name>
    <size width="900" height="600" />
    <location x="100" y="100" />
    <title>Persistit AdminUI Help</title>
    <image>toplevelfolder</image>
    <toolbar>
      <helpaction>javax.help.BackAction</helpaction>
      <helpaction>javax.help.ForwardAction</helpaction>
      <helpaction>javax.help.SeparatorAction</helpaction>
      <helpaction>javax.help.HomeAction</helpaction>
      <helpaction>javax.help.ReloadAction</helpaction>
      <helpaction>javax.help.SeparatorAction</helpaction>
      <helpaction>javax.help.PrintAction</helpaction>
      <helpaction>javax.help.PrintSetupAction</helpaction>
    </toolbar>
  </presentation>

<!--
  <presentation>
     <name>main</name>
     <size width="600" height="500" />
     <location x="300" y="200" />
     <title>Persistit AdminUI - Online Help</title>
  </presentation>
-->

</helpset>
