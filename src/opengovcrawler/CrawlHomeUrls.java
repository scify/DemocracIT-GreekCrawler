/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Starts crawling procedure for each base url for each Ministry.
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class CrawlHomeUrls {

    public static int DEFAULT_TIMEOUT = 20000;
    public static int DEFAULT_CONSULTATION_TIMEOUT = 600000;
    public static int NUM_OF_CONSULTATION_THREADS = 1;
    public static int module_id = 1; // 1 = Crawler module
    public static int action_id = 2; // 2 = Consultation_Crawling
    public static int status_code_error = 3; // 3 = Consultation_Crawling Error
    public static int status_code_success = 2; // 2 = Consultation_Crawling Succesful
    public static long lStartTime;
    public static long lEndTime;
    public static long difference;
    public static int crawlerId;

    /**
     * Checks if all retries for every consultation that failed to be crawled
     * have finished.
     *
     * @param retries
     * @return True if all retries have finished or False if not
     */
    public boolean allDone(int[] retries) {
        for (int num : retries) {
            if (num != -1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Gets a map of Ministry names and their corresponding base urls and starts
     * a threaded crawling of the consultations of each base url.
     *
     * @param ministryBaseUrlMap - Map of ministries and a set of their base urls.
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     */
    public void CrawlCollapsedUrls(Map<String, Set<String>> ministryBaseUrlMap) throws InterruptedException, IOException, SQLException {

        Map<String, Set<String>> ConsUrlsPerOrgInit = GetConsLinksPerOrg(ministryBaseUrlMap);
        ExecutorService executor = Executors.newFixedThreadPool(GetMinistries.NUM_OF_CONS_THREADS);

        // START TIMER
        lStartTime = System.currentTimeMillis();
        System.out.println("@ " + new Date(lStartTime));
        System.out.println(GetMinistries.NUM_OF_CONS_THREADS + " : Threads have been started.");
        crawlerId = DB.LogCrawler(lStartTime);

        int consFailed = 0;
        JSONObject obj = new JSONObject();

        ArrayList<String> unprocessed = new ArrayList<>();
        for (Object x : ConsUrlsPerOrgInit.keySet()) { //for each organization
            //FIRST GET THE MINISTRY ID (DB_KEY)
            int orgID = DB.GetOrganizationId(x);
            //THEN PROCEED
            Set<String> y = (Set<String>) ConsUrlsPerOrgInit.get(x);
            for (String z : y) { //for each consultation
                Runnable worker = new ConsultationThreadedCrawling((String) z, unprocessed, orgID);
                executor.execute(worker);
            }

            long failStartTime = System.currentTimeMillis();
            int[] retries = new int[unprocessed.size()];
            while (!allDone(retries)) {
                executor = Executors.newFixedThreadPool(GetMinistries.NUM_OF_CONS_THREADS);
                ArrayList<String> dummy = new ArrayList<>();
                for (int i = 0; i < retries.length; i++) {
                    if (retries[i] < 5 && retries[i] >= 0) {
                        Runnable worker = new ConsultationThreadedCrawling((String) unprocessed.get(i), dummy, retries, i, orgID);
                        executor.execute(worker);
                    }
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(2400, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Logger.getLogger(CrawlHomeUrls.class.getName()).log(Level.SEVERE, e.getMessage());
                }
            }

            for (int i = 0; i < retries.length; i++) {
                if (retries[i] == 5) {
                    String unFetchedCon = unprocessed.get(i);
                    long failEndTime = System.currentTimeMillis();
                    DB.LogUnprocessedConsultations(failStartTime, failEndTime, unFetchedCon, crawlerId, status_code_error);
                    consFailed++;
                }
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(2400, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Logger.getLogger(CrawlHomeUrls.class.getName()).log(Level.SEVERE, e.getMessage());
        }

        while (!executor.isTerminated()) {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        }
        // END TIMER
        lEndTime = System.currentTimeMillis();
        difference = lEndTime - lStartTime;
        System.out.println("Crawling FINISHED!");
        System.out.println("Elapsed time for this crawling was: " + (difference / 1000) / 60 + " minutes.");
        /*  IF ALL CONSULTATIONS AND ARTICLES INSERTED CORRECT
         THEN STATUS_ID=2 ELSE STATUS_ID=3
         */
        int status_id;
        String message = null;
        if (consFailed > 0) {
            status_id = 3;
            obj.put("message", "Some errors occured during crawling.");
            obj.put("details", consFailed + " unprossed consultations exist.");
//            message = consFailed + " unprossed consultations exist.";
        } else {
            status_id = 2;
            obj.put("message", "Crawling completed.");
            obj.put("details", "");
//            message = "Crawling completed.";
        }
        DB.UpdateLogCrawler(lEndTime, status_id, crawlerId, obj);
    }

    /**
     * Gets a map of Ministry names and their corresponding base urls and clears
     * the base urls, keeping only those that actually contain the
     * consultations.
     *
     * @param ministryBaseUrlMap - Map of ministries and a set of their base urls.
     * @return ConsUrlsPerOrg - A Map of (Ministry,
     * Set(ActualConsultationBaseUrl))
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     */
    public Map<String, Set<String>> GetConsLinksPerOrg(Map<String, Set<String>> ministryBaseUrlMap) throws IOException, SQLException {
        Map<String, Set<String>> ConsUrlsPerOrg = new HashMap<>();
        for (Object x : ministryBaseUrlMap.keySet()) {
            Set<String> y = (Set<String>) ministryBaseUrlMap.get(x);
            for (String z : y) {
                if (!z.toString().equals("home")) {
                    try {
                        DB.UpdateOrganizationUrls(x, "http://www.opengov.gr/" + z);
                        Document baseSite = Jsoup.connect("http://www.opengov.gr/" + z).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").get();
                        Elements consultaionsUrls = baseSite.select("div.index_list_item_object").select("a");//select("index_list").
                        Set<String> ur = new HashSet<>();
                        for (Element conurl : consultaionsUrls) {
                            if (Character.isDigit(conurl.attr("href").charAt(conurl.attr("href").length() - 1))) {
                                ur.add(conurl.attr("href"));
                            }
                        }
                        ConsUrlsPerOrg.put((String) x, ur);
                    } catch (Exception ex) {
                        System.err.println("http://www.opengov.gr/" + z);
                    }
                }
            }
        }
        return ConsUrlsPerOrg;
    }
}
