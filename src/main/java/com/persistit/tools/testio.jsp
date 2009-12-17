<%@ page language="java" import="java.io.*" %>

<HTML>
<HEAD>
<STYLE>
<!--
BODY           {background-color: #FFFFFF;
                font-size:80%; font-family: Verdana, Helvetica, Arial, sans-serif  }
-->
</STYLE>
<SCRIPT>

</SCRIPT>
</HEAD>
<BODY>
The ServletContext.getRealPath("/data") is: 
<%
  ServletContext context = request.getSession().getServletContext();
	String fileName = request.getParameter("path");
	if (fileName == null || fileName.length() == 0)
	{
		fileName = "data/test.txt";
	}
	fileName = context.getRealPath(fileName);
  out.println(fileName);
	boolean isWrite = "yes".equals(request.getParameter("write"));
%>
Here's what happens when you try to open and read from:
<br />
<%= fileName %>
<br />
<%
if (!isWrite)
{
  %><br />Attempting to read from file: <%=fileName%><br /><%
	BufferedReader reader = new BufferedReader(new FileReader(fileName));
	for (int index = 0; index < 10; index++)
	{
		String line = reader.readLine();
		if (line == null) break;
		%>
		<%= line %><br />
		<%
	}
	reader.close();
}
else
{
  %><br />Attempting to write to file: <%=fileName%><br /><%
	PrintWriter writer = new PrintWriter(new FileWriter(fileName));
	for (int index = 0; index < 10; index++)
	{
		writer.println("Line " + index + " in " + fileName);
	}
	writer.close();
}
%>
</BODY>
</HTML>