/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TreeMap;
import static opengovcrawler.CrawlHomeUrls.DEFAULT_TIMEOUT;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Retrieves the consultations of each Ministry and their urls.
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class GetConsultationsList {

    TreeMap<String, ArrayList<ConsultationsList>> consultationListPerMinistry = new TreeMap<String, ArrayList<ConsultationsList>>();

    /**
     * Gets a map of Ministries and their corresponding urls and returns a list
     * of the currently available consultations, with their urls, for each Ministry.
     *
     * @param ministries A TreeMap of (MinistryName, PageUrl)
     * @return consultationListPerMinistry - A TreeMap of (MinistryName, Set(ConsultaionUrl))
     * @throws IOException
     * @throws java.lang.InterruptedException
     * @throws java.sql.SQLException
     */
    public TreeMap GetConsultationList(TreeMap<String, String> ministries) throws IOException, InterruptedException, SQLException {
        for (String mr : ministries.keySet()) {
            // Keep the Ministry name and url
            String ministryName = mr;
            String ministryUrl = ministries.get(mr);
            // Connect to each ministry and get the parsed html
            Document doc = Jsoup.connect(ministryUrl).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").get();
            // Get a list of elements, that refer to each consultaion link
            Elements consultationHrefs = doc.select("div.index_listing.downspace_item.archive_listing").select("li").select("p").select("a");

            // Check if the minisrty has more than one page with consultaion listings.
            // If NO, then we create a new ConsultationList object and keep the 
            // consultation's title and url for each ministry (that we have already parsed).
            if ((doc.getElementsByClass("pages").text().length()) == 0) {
                int currentPage = 1;
                int lastPage = 1;
                for (Element link : consultationHrefs) {
                    ConsultationsList conList = new ConsultationsList();
                    conList.consultationTitle = link.text();
                    conList.consultationUrl = link.attr("href");
                    // Check if ministry name already exists in the map or not
                    if (!consultationListPerMinistry.containsKey(ministryName)) {
                        consultationListPerMinistry.put(ministryName, new ArrayList<ConsultationsList>());
                        consultationListPerMinistry.get(ministryName).add(conList);
                    } else {
                        consultationListPerMinistry.get(ministryName).add(conList);
                    }
                }
            }
            // If YES (there are multiple pages with consultations), then we make a call
            // to each pages of consultations and add them again (title and url) in the map
            else {
                String baseUrl = doc.baseUri();
                String navigationPagesTitle = doc.getElementsByClass("pages").text();
                String[] numOfTotalPages = navigationPagesTitle.substring(navigationPagesTitle.length() - 2).split(" ");
                int lastPage = Integer.parseInt(numOfTotalPages[1]);

                for (Element link : consultationHrefs) {
                    ConsultationsList conList = new ConsultationsList();
                    conList.consultationTitle = link.text();
                    conList.consultationUrl = link.attr("href");
                    // Check if ministry name already exists in the map or not
                    if (!consultationListPerMinistry.containsKey(ministryName)) {
                        consultationListPerMinistry.put(ministryName, new ArrayList<ConsultationsList>());
                        consultationListPerMinistry.get(ministryName).add(conList);
                    } else {
                        consultationListPerMinistry.get(ministryName).add(conList);
                    }
                }

                String nextPostLink_1 = doc.select("div.wp-pagenavi").select("a").last().attr("href");
                String[] numOfCurrentPage = doc.getElementsByClass("current").text().split(" ");
                int currentPage = Integer.parseInt(numOfCurrentPage[0]);
                for (int page = 2; page <= lastPage; page++) {
                    String currentUrl = baseUrl + "/page/" + page;
                    Document doc2 = Jsoup.connect(currentUrl).userAgent("Mozilla").get();
                    Elements consultationHrefs2 = doc2.select("div.index_listing.downspace_item.archive_listing").select("li").select("p").select("a");
                    for (Element link : consultationHrefs2) {
                        ConsultationsList conList = new ConsultationsList();
                        conList.consultationTitle = link.text();
                        conList.consultationUrl = link.attr("href");
                        // Check if ministry name already exists in the map or not
                        if (!consultationListPerMinistry.containsKey(ministryName)) {
                            consultationListPerMinistry.put(ministryName, new ArrayList<ConsultationsList>());
                            consultationListPerMinistry.get(ministryName).add(conList);
                        } else {
                            consultationListPerMinistry.get(ministryName).add(conList);
                        }
                    }
                }
            }
        }
        CollapseUrls clearUrls = new CollapseUrls();
        clearUrls.CollapseUrls(consultationListPerMinistry);
        return consultationListPerMinistry;
    }
}
