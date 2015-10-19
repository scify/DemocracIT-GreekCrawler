/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import static opengovcrawler.CrawlHomeUrls.DEFAULT_TIMEOUT;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Performs the threaded crawling procedure for each consultation.
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class ConsultationThreadedCrawling implements Runnable {

    private String consultationPage;
    private ArrayList<String> unprocessed;
    private int[] retries;
    private int pos;
    private int orgId;
    public Article curArticle;
    public ArrayList<Article> articlesList;
    public static int NUM_OF_ARTICLE_THREADS = 1;
    public static int MAX_RETRIES = 5;
    public static int module_id = 1; // 1 = Crawler module
    public static int action_id = 3; // 3 = Article_Crawling
    public static int status_code = 3; // 3 = Article_Crawling Error
    public static int newComments;

    /**
     * Constructor for the initial consultation crawling.
     *
     * @param s  - The consultation page url
     * @param unprocessed - Stores the unprocessed consultations
     * @param orgId - The ministry id from the DB
     */
    public ConsultationThreadedCrawling(String s, ArrayList<String> unprocessed, int orgId) {
        this.consultationPage = s;
        this.unprocessed = unprocessed;
        this.orgId = orgId;
    }

    /**
     * Constructor for the recurring crawling of the failed to crawl consultations.
     *
     * @param s  - The consultation page url
     * @param unprocessed - Stores the unprocessed consultations
     * @param orgId - The ministry id from the DB
     * @param retries - Stores the retries for each failed consultation url
     */
    public ConsultationThreadedCrawling(String s, ArrayList<String> unprocessed, int[] retries, int pos, int orgId) {
        this.consultationPage = s;
        this.unprocessed = unprocessed;
        this.retries = retries;
        this.pos = pos;
        this.orgId = orgId;
    }

    /**
     * Checks if all articles of a consultation have finished crawling.
     *
     * @param retryArticles - Stores the retries for a consultation's articles
     * @return True if all retries have finished or False if  not
     */
    public boolean allArticlesDone(int[] retryArticles) {
        for (int num : retryArticles) {
            if (num != -1 && num != MAX_RETRIES) {
                return false;
            }
        }
        return true;
    }

    /**
     * Counts the number of broken articles
     *
     * @param retryArticles -  - Stores the retries for a consultation's articles
     * @return - The number of unprocessed articles
     */
    public int numArticlesBroken(int[] retryArticles) {
        int num = 0;
        for (int retries : retryArticles) {
            if (retries == MAX_RETRIES) {
                num++;
            }
        }
        return num;
    }

    @Override
    public void run() {
        try {
            boolean isInSkip = DB.CheckSkipList(consultationPage);
            if (false == isInSkip) {
                String dbStatus = DB.GetConsultationStatus(consultationPage);
                switch (dbStatus) {
                    case "blue":
                        return;
                    case "red":
                        partialCrawling(dbStatus);
                        break;
                    default:
                        fullCrawling(dbStatus);
                        break;
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(ConsultationThreadedCrawling.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    /**
     * Performs full consultation crawling in case the consultations is new or is still open to discussion.
     *
     * @param dbStatus - The current status of the consultation in the DB
     */
    public void fullCrawling(String dbStatus) {
        try {
            articlesList = new ArrayList<Article>();
            Document conBaseUrl = Jsoup.connect(this.consultationPage).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").get();

            String status = dbStatus;
            String reportText = null;
            String reportURL = null;
            String completedText = null;
            long successStartTime = System.currentTimeMillis();

            // Get consultation's status, report_text & report_url
            Elements consComplete = conBaseUrl.select("div.post_content.is_complete");
            if (consComplete.size() > 0) {
                completedText = "";
                for (Element consNode : consComplete) {
                    completedText += consNode.html();
                }
                status = "red";
            }
            Elements results = conBaseUrl.select("div.results");
            if (results.size() > 0) {
                reportText = "";
                Elements resultsHTMLNodes = results.select("div.post_content.has_results");
                for (Element htmlNode : resultsHTMLNodes) {
                    reportText += htmlNode.html();
                    status = "blue";
                }
                Elements links = resultsHTMLNodes.select("a");
                for (Element link : links) {
                    if (link.attr("href").endsWith("pdf")) {
                        reportURL = link.attr("href");
                    }
                }
            }
            // Get consultation's title
            String consTitle = "";
            Elements consHeader = conBaseUrl.select("div.post.clearfix").select("h3").not("h3.complete");
            consTitle += consHeader.last().html();

            // Get consultation's description text
            String consBody = "";
            Elements consIntroText = conBaseUrl.select("div.post_content").not("div.post_content.is_complete").not("div.post_content.has_results");
            for (Element introNode : consIntroText) {
                consBody += introNode.html();
            }

            // Get consultation's startDate and endDate
            Elements Dates = conBaseUrl.select("div#sidebar").select("div.sidespot.red_spot").select("h4").select("span");
            String startDate = null;
            String endDate = null;
            if (Dates.get(0).text().equals("")) {
                startDate = "N / A";
            } else {
                startDate = Dates.get(0).text();
            }
            if (Dates.get(1).text().equals("")) {
                endDate = "N / A";
            } else {
                endDate = Dates.get(1).text();
            }

            //Get consultation's list of articles
            Elements articleLinks = conBaseUrl.select("div#consnav").select("li");
            int order = 0;
            for (Element artLink : articleLinks) {
                String artUrl = artLink.select("a.list_comments_link").attr("href");
                String artTitle = artLink.select("a.list_comments_link").text();
                order++;
                // Check for open or closed comments
                Boolean artHasComments = Boolean.TRUE;
                int numOfComments;
                int commentPages = -1;
                String commentsUrl = null;
                if (artLink.select("span.list_comments").select("span").first().text().equals("Σχόλια κλειστά")) {
                    artHasComments = Boolean.FALSE;
                    numOfComments = 0;
                    commentsUrl = "-";
                } else {
                    artHasComments = Boolean.TRUE;
                    String[] split = artLink.select("span.list_comments").select("a").text().split(" ");
                    commentsUrl = artLink.select("span.list_comments").select("a").attr("href");
                    if (split[0].contains(".")) {
                        numOfComments = Integer.parseInt(split[0].replaceAll("\\.", ""));
                    } else {
                        numOfComments = Integer.parseInt(split[0]);
                    }
                    commentPages = numOfComments / 50 + 1;
                }
                curArticle = new Article(artUrl, artTitle, artHasComments, numOfComments, commentsUrl, commentPages, order);
                articlesList.add(curArticle);
            }
            // Create the consultation, get it's id from db and insert it if not exists
            Consultation currentCons = new Consultation(consultationPage, consTitle, consBody, startDate, endDate, articlesList, status, completedText, reportText, reportURL);
            int consID = DB.GetConsultationId(consultationPage);
            if (consID == -1) {
                // call insert to db method
                consID = DB.InsertNewConsultation(currentCons, orgId, articlesList.size());
            }

            // Get consultation's referenced material
            Elements referencedMaterial = conBaseUrl.select("div#sidebar").select("div.sidespot.orange_spot").select("span.file").select("a");
            if (referencedMaterial.size() > 0) {
                for (Element refMat : referencedMaterial) {
                    String refMaterialTitle = refMat.text();
                    String refMaterialFakeUrl = refMat.attr("href");
                    Connection conRef = Jsoup.connect(refMaterialFakeUrl).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").ignoreContentType(true);
                    Connection.Response resp = null;
                    try {
                        resp = conRef.execute();
                    } catch (HttpStatusException ex) {
                        System.err.println("Failed for " + refMaterialFakeUrl);
                    }
                    Document conRefMaterial;
                    if (resp != null && resp.statusCode() == 200) {
                        conRefMaterial = conRef.get();
                        String refMaterialActualUrl = conRefMaterial.baseUri();
                        //Create an MD5 hash from the actual url to use as the pdf's filename
                        String md5hash = PdfFiles.CreateMD5Hash(refMaterialActualUrl);
                        // Save pdf to local folder and get the file's relative path
                        String relativePath = PdfFiles.SavePdfToFileSystem(refMaterialActualUrl, md5hash);
                        // Create the ReferencedPdf object
                        ReferencedPdf curPdf = new ReferencedPdf(refMaterialTitle, refMaterialFakeUrl, refMaterialActualUrl, md5hash, relativePath);
                        //Insert pdf info into db
                        DB.InsertRelevantMaterial(curPdf, consID);
                    }
                }
            }

            ExecutorService executor = Executors.newFixedThreadPool(GetMinistries.NUM_OF_ART_THREADS);
            ArrayList<Article> unprocessedArticles = new ArrayList<>();
            for (Article a : articlesList) {
                Runnable worker = new ArticleThreadedCrawling(a, unprocessedArticles, consID);
                executor.execute(worker);
            }
            executor.shutdown();

            try {
                executor.awaitTermination(2400, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Logger.getLogger(ConsultationThreadedCrawling.class.getName()).log(Level.SEVERE, "*" + this.consultationPage);
            }
            long failStartTime = System.currentTimeMillis();
            int[] retryArticles = new int[unprocessedArticles.size()];
            while (!allArticlesDone(retryArticles)) {
                executor = Executors.newFixedThreadPool(GetMinistries.NUM_OF_ART_THREADS);
                ArrayList<Article> dummyArt = new ArrayList<>();
                for (int i = 0; i < retryArticles.length; i++) {
                    if (retryArticles[i] < MAX_RETRIES && retryArticles[i] >= 0) {
                        Runnable worker2 = new ArticleThreadedCrawling((Article) unprocessedArticles.get(i), dummyArt, retryArticles, i, consID);
                        executor.execute(worker2);
                    }
                }
                executor.shutdown();
                executor.awaitTermination(2400, TimeUnit.SECONDS);
            }
            for (int i = 0; i < retryArticles.length; i++) {
                if (retryArticles[i] == MAX_RETRIES) {
                    String unFetchedArt = unprocessedArticles.get(i).url;
                    long failEndTime = System.currentTimeMillis();
                    DB.LogUnprocessedArticles(failStartTime, failEndTime, unFetchedArt, CrawlHomeUrls.crawlerId, status_code);
                }
            }
            //update the consultation status in db if needed
            // call update record to db method
            DB.UpdateConsultation(currentCons, orgId, consID, articlesList.size());
            long successEndTime = System.currentTimeMillis();
            DB.LogProcessedConsultation(successStartTime, successEndTime, currentCons.url, CrawlHomeUrls.crawlerId, CrawlHomeUrls.status_code_success);
        } catch (Exception ex) {
            Logger.getLogger(ConsultationThreadedCrawling.class.getName()).log(Level.SEVERE, "*" + this.consultationPage);
            Logger.getLogger(ConsultationThreadedCrawling.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);

            if (retries != null) {
                retries[pos]++;
            } else {
                this.unprocessed.add(consultationPage);
            }
        }
    }

    /**
     * Performs partial consultation crawling in case the consultation is already
     * in the DB but its status was "Closed", so no need for crawling its articles and their comments again.
     *
     * @param dbStatus - The current status of the consultation in the DB
     */
    public void partialCrawling(String dbStatus) {
        try {
            articlesList = new ArrayList<Article>();
            Document conBaseUrl = Jsoup.connect(this.consultationPage).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").get();

            String status = dbStatus;
            String reportText = null;
            String reportURL = null;
            String completedText = null;
            long successStartTime = System.currentTimeMillis();

            // Get consultation's status, report_text & report_url
            Elements consComplete = conBaseUrl.select("div.post_content.is_complete");
            if (consComplete.size() > 0) {
                completedText = "";
                for (Element consNode : consComplete) {
                    completedText += consNode.html();
                }
                status = "red";
            }
            Elements results = conBaseUrl.select("div.results");
            if (results.size() > 0) {
                reportText = "";
                Elements resultsHTMLNodes = results.select("div.post_content.has_results");
                for (Element htmlNode : resultsHTMLNodes) {
                    reportText += htmlNode.html();
                    status = "blue";
                }
                Elements links = resultsHTMLNodes.select("a");
                for (Element link : links) {
                    if (link.attr("href").endsWith("pdf")) {
                        reportURL = link.attr("href");
                    }
                }
            }
            // Get consultation's title
            String consTitle = "";
            Elements consHeader = conBaseUrl.select("div.post.clearfix").select("h3").not("h3.complete");
            consTitle += consHeader.last().html();

            // Get consultation's description text
            String consBody = "";
            Elements consIntroText = conBaseUrl.select("div.post_content").not("div.post_content.is_complete").not("div.post_content.has_results");
            for (Element introNode : consIntroText) {
                consBody += introNode.html();
            }

            // Get consultation's startDate and endDate
            Elements Dates = conBaseUrl.select("div#sidebar").select("div.sidespot.red_spot").select("h4").select("span");
            String startDate = null;
            String endDate = null;
            if (Dates.get(0).text().equals("")) {
                startDate = "N / A";
            } else {
                startDate = Dates.get(0).text();
            }
            if (Dates.get(1).text().equals("")) {
                endDate = "N / A";
            } else {
                endDate = Dates.get(1).text();
            }

            //Get consultation's list of articles
            Elements articleLinks = conBaseUrl.select("div#consnav").select("li");
            int order = 0;
            for (Element artLink : articleLinks) {
                String artUrl = artLink.select("a.list_comments_link").attr("href");
                String artTitle = artLink.select("a.list_comments_link").text();
                order++;
                // Check for open or closed comments
                Boolean artHasComments = Boolean.TRUE;
                int numOfComments;
                int commentPages = -1;
                String commentsUrl = null;
                if (artLink.select("span.list_comments").select("span").first().text().equals("Σχόλια κλειστά")) {
                    artHasComments = Boolean.FALSE;
                    numOfComments = 0;
                    commentsUrl = "-";
                } else {
                    artHasComments = Boolean.TRUE;
                    String[] split = artLink.select("span.list_comments").select("a").text().split(" ");
                    commentsUrl = artLink.select("span.list_comments").select("a").attr("href");
                    if (split[0].contains(".")) {
                        numOfComments = Integer.parseInt(split[0].replaceAll("\\.", ""));
                    } else {
                        numOfComments = Integer.parseInt(split[0]);
                    }
                    commentPages = numOfComments / 50 + 1;
                }
                curArticle = new Article(artUrl, artTitle, artHasComments, numOfComments, commentsUrl, commentPages, order);
                articlesList.add(curArticle);
            }
            Consultation currentCons = new Consultation(consultationPage, consTitle, consBody, startDate, endDate, articlesList, status, completedText, reportText, reportURL);
            int consID = DB.GetConsultationId(consultationPage);

            // Get consultation's referenced material
            Elements referencedMaterial = conBaseUrl.select("div#sidebar").select("div.sidespot.orange_spot").select("span.file").select("a");
            if (referencedMaterial.size() > 0) {
                for (Element refMat : referencedMaterial) {
                    String refMaterialTitle = refMat.text();
                    String refMaterialFakeUrl = refMat.attr("href");
                    Connection conRef = Jsoup.connect(refMaterialFakeUrl).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").ignoreContentType(true);
                    Connection.Response resp = null;
                    try {
                        resp = conRef.execute();
                    } catch (HttpStatusException ex) {
                        System.err.println("Failed for " + refMaterialFakeUrl);
                    }
                    Document conRefMaterial;
                    if (resp != null && resp.statusCode() == 200) {
                        conRefMaterial = conRef.get();
                        String refMaterialActualUrl = conRefMaterial.baseUri();
                        //Create an MD5 hash from the actual url to use as the pdf's filename
                        String md5hash = PdfFiles.CreateMD5Hash(refMaterialActualUrl);
                        // Save pdf to local folder and get the file's relative path
                        String relativePath = PdfFiles.SavePdfToFileSystem(refMaterialActualUrl, md5hash);
                        // Create the ReferencedPdf object
                        ReferencedPdf curPdf = new ReferencedPdf(refMaterialTitle, refMaterialFakeUrl, refMaterialActualUrl, md5hash, relativePath);
                        //Insert pdf info into db
                        DB.InsertRelevantMaterial(curPdf, consID);
                    }
                }
            }

            // call update record to db method
            if (dbStatus.equals(status)) {
                return;
            } else {
                DB.UpdateConsultationStatus(currentCons, orgId, consID);
                long successEndTime = System.currentTimeMillis();
                DB.LogProcessedConsultation(successStartTime, successEndTime, currentCons.url, CrawlHomeUrls.crawlerId, CrawlHomeUrls.status_code_success);
            }
        } catch (Exception ex) {
            Logger.getLogger(ConsultationThreadedCrawling.class.getName()).log(Level.SEVERE, "*" + this.consultationPage);
            Logger.getLogger(ConsultationThreadedCrawling.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    
    @Override
    public String toString() {
        return this.consultationPage;
    }

}