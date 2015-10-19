package opengovcrawler;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.TreeMap;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Retrieves the Ministry names and their corresponding hyperlink from
 * opengov.gr
 *
 * @author Christos Sardianos
 * @version 1.0 - 20/02/2015
 */
public class GetMinistries {

    public static TreeMap<String, String> ministries;
    public static String configFile;
    public static int NUM_OF_CONS_THREADS;
    public static int NUM_OF_ART_THREADS;

    /**
     * Starts the crawling process of OpenGov and crawls the Ministry names and
     * their urls.
     *
     * @param args The file name of the configuration file (ex. crawler.conf)
     * @throws IOException
     * @throws java.lang.InterruptedException
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        args = new String[1];
        args[0] = "config.properties";
        if (args.length > 1) {
            System.out.println();
            System.out.println("**************************************************************************************");
            System.out.println();
            System.out.println("Error:  You have specified too many arguments!");
            System.out.println("        Please provide ONLY the configuration file name as the first AND only argument.");
            System.out.println();
            System.out.println("**************************************************************************************");
            System.out.println();
            System.exit(1);
        }

        if (args.length == 0) {
            System.out.println();
            System.out.println("*********************************************************************************");
            System.out.println();
            System.out.println("Error:  No configuration file name was provided!");
            System.out.println("        Please provide the configuration file name as the first AND only argument.");
            System.out.println();
            System.out.println("*********************************************************************************");
            System.out.println();
            System.exit(1);
        }

        configFile = args[0];
        BufferedReader br = null;
        String line = "";
        String splitBy = "=";
        try {
            br = new BufferedReader(new FileReader(configFile));
            while ((line = br.readLine()) != null) {
                if (line.startsWith("NUM_OF_CONS_THREADS")) {
                    String[] lineParts = line.split(splitBy, 2);
                    NUM_OF_CONS_THREADS = Integer.parseInt(lineParts[1]);
                } else if (line.startsWith("NUM_OF_ART_THREADS")) {
                    String[] lineParts = line.split(splitBy, 2);
                    NUM_OF_ART_THREADS = Integer.parseInt(lineParts[1]);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println();
            System.out.println("*****************************************************************");
            System.out.println();
            System.out.println("Error:  The specified configuration file name does not exist!");
            System.out.println("        Please specify a correct configuration file and try again.");
            System.out.println();
            System.out.println("*****************************************************************");
            System.out.println();
//            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        Properties.verbose = true;
        ministries = new TreeMap<String, String>();

        try {
            DB.init();
            System.out.println("Crawling STARTED!");
            Document doc = Jsoup.connect("http://www.opengov.gr/home").timeout(CrawlHomeUrls.DEFAULT_TIMEOUT).userAgent("Mozilla").get();
            String title = doc.title();
            Elements ministryCategories = doc.select("div.ministeries.downspace_item");
            Elements ministriesAhrefs = null;
            HashSet ogReadMins = new HashSet();
            for (Element minCategory : ministryCategories) {
                String minGroup = minCategory.select("div.downspace_item_title").text();
                ministriesAhrefs = minCategory.select("div.downspace_item_content.side_list").select("li").select("a");
                for (Element link : ministriesAhrefs) {
                    ministries.put(link.text(), link.attr("href"));
                    DB.InsertOrganization(link.text(), link.attr("href"), minGroup);
                    ogReadMins.add(link.text());
                }
            }
            DB.UpdateGroupOfRemovedMinitries(ogReadMins);
            GetConsultationsList consultationList = new GetConsultationsList();
            consultationList.GetConsultationList(ministries);
        } catch (HttpStatusException ht) {
            ht.printStackTrace();
            System.out.println(ht);
        }
    }
}
