/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Retrieves the base urls for each Ministry.
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class CollapseUrls {

    /**
     * Gets a map of Ministries and their corresponding list of consultation urls
     * and creates a mapping of the name and base url for each Ministry.
     *
     * @param consultationListPerMinistry - Map of ministry and its consultations list
     * @throws IOException
     * @throws java.lang.InterruptedException
     * @throws java.sql.SQLException
     */
    public void CollapseUrls(TreeMap<String, ArrayList<ConsultationsList>> consultationListPerMinistry) throws InterruptedException, IOException, SQLException {

        Map ministryBaseUrlMap = new HashMap();
        for (String entry : consultationListPerMinistry.keySet()) {
            ArrayList<ConsultationsList> cl = consultationListPerMinistry.get(entry);
            Set<String> bsUrls = new HashSet<>();
            for (ConsultationsList c : cl) {
                String[] urlParts = c.consultationUrl.split("/");
                String baseUrl = urlParts[3];
                if (!bsUrls.contains(baseUrl)) {
                    bsUrls.add(baseUrl);
                    ministryBaseUrlMap.put(entry, bsUrls);
                }
            }
        }
        CrawlHomeUrls crawlConsultations = new CrawlHomeUrls();
        crawlConsultations.CrawlCollapsedUrls(ministryBaseUrlMap);
    }

}
