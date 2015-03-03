
package opengovcrawler;

/**
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class ReferencedPdf {

    public String refMaterialTitle;
    public String refMaterialFakeUrl;
    public String refMaterialActualUrl;
    public String md5hash;
    public String relativePath;

    /**
     * Constructor of article's referenced pdf.
     *
     * @param refMaterialTitle - The title of the pdf file.
     * @param refMaterialFakeUrl - The initial pdf's url before redirection to the pdf.
     * @param refMaterialActualUrl - The actual pdf's url after redirection to the pdf.
     * @param md5hash - The md5 hash from the pdf's actual url.
     * @param relativePath - The file system's relative path of the local pdf copy.
     */
    public ReferencedPdf(String refMaterialTitle, String refMaterialFakeUrl, String refMaterialActualUrl, String md5hash, String relativePath) {
        this.refMaterialTitle = refMaterialTitle;
        this.refMaterialFakeUrl = refMaterialFakeUrl;
        this.refMaterialActualUrl = refMaterialActualUrl;
        this.md5hash = md5hash;
        this.relativePath = relativePath;
    }
}
