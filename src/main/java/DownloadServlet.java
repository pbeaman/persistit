import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;
import com.persistit.tools.PersistitWebDataBean;
/**
 * @author Peter Beaman
 * @version 1.0
 */
public class DownloadServlet
extends HttpServlet
{
    
    private final static String ZIP_MIME_TYPE = "application/x-zip-compressed";

    public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException
    {
        PersistitWebDataBean.doGetForDownload(request, response);
    }
    
    public void destroy()
    {
        super.destroy();
        try
        {
        	PersistitWebDataBean.close();
        }
        catch (PersistitException e)
        {
            e.printStackTrace();
        }
    }
    

}
