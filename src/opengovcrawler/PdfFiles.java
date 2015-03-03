
package opengovcrawler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Creates the MD5Hash for the pdf file and stores it to the local file system.
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class PdfFiles {

    /**
     * Creates the MD5Hash string from the pdf's actual url
     *
     * @param refMaterialActualUrl - The actual pdf's url after redirection to the pdf.
     * @return - The created MD5Hash of the pdf's actual url.
     * @throws java.security.NoSuchAlgorithmException 
     */
    public static String CreateMD5Hash(String refMaterialActualUrl) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(refMaterialActualUrl.getBytes());
        byte byteData[] = md.digest();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    /**
     * Saves a local copy of the consultation's referenced pdf.
     *
     * @param refMaterialActualUrl -  - The actual pdf's url after redirection to the pdf.
     * @param md5hash - The created MD5Hash of the pdf's actual url.
     * @return  - The file system's relative path of the local pdf copy.
     * @throws java.net.MalformedURLException
     */
    public static String SavePdfToFileSystem(String refMaterialActualUrl, String md5hash) throws MalformedURLException, IOException {
        URL url = new URL(refMaterialActualUrl);
        File file = new File("PDFs");
        if (!file.exists()) {
            file.mkdir();
        }
        String relativePath = file + "/" + md5hash + ".pdf";
        InputStream in = url.openStream();
        File f = new File(relativePath);

        if (!f.exists()) {
            FileOutputStream fos = new FileOutputStream(f);
            int length = -1;
            byte[] buffer = new byte[1024];
            while ((length = in.read(buffer)) > -1) {
                fos.write(buffer, 0, length);
            }
            fos.close();
            in.close();
        }
        return relativePath;
    }

}