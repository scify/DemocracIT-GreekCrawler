/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import static opengovcrawler.CrawlHomeUrls.DEFAULT_TIMEOUT;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Performs the threaded crawling procedure for each consultation's corresponding articles and comments.
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class ArticleThreadedCrawling implements Runnable {

    private Article article;
    private ArrayList<Article> unprocessedArticles;
    private int[] retryArticles;
    private int artPos;
    public int consID;

    /**
     * Constructor for the initial article crawling.
     *
     * @param article  - The article page url
     * @param unprocessedArticles  - Stores the unprocessed articles
     * @param consID  - The consultation id from the DB
     */
    public ArticleThreadedCrawling(Article article, ArrayList<Article> unprocessedArticles, int consID) {
        this.article = article;
        this.unprocessedArticles = unprocessedArticles;
        this.consID = consID;
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Constructor for the recurring crawling of the failed to crawl articles.
     *
     * @param article  - The article page url
     * @param unprocessedArticles  - Stores the unprocessed articles
     * @param consID  - The consultation id from the DB
     * @param retryArticles  - Stores the retries for each failed article url
     */
    public ArticleThreadedCrawling(Article article, ArrayList<Article> unprocessedArticles, int[] retryArticles, int artPos, int consID) {
        this.article = article;
        this.unprocessedArticles = unprocessedArticles;
        this.retryArticles = retryArticles;
        this.artPos = artPos;
        this.consID = consID;
    }

    @Override
    public void run() {
        Article artToCrawl = this.article;
        try {
            long start = System.currentTimeMillis();
            Document artPage = Jsoup.connect(artToCrawl.url).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").get();
            String articleBody = artPage.select("div.post.clearfix").select("div.post_content").html();//text();
            artToCrawl.content = articleBody;
            int articleDbId = 0;
            try {
                // Insert each article of the consultation into db.
                articleDbId = DB.InsertArticles(artToCrawl, consID);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

            // Continue crawling comments (if any)
            if (artToCrawl.hasComments) {
                Elements commentsList = artPage.select("div#content").select("div#comments").select("ul.comment_list").select("li");
                for (Element nextComment : commentsList) {
                    String[] userString = nextComment.select("div.user").select("div.author").text().split("\\|");
                    String commentTimestamp = userString[0];
                    String commentAuthor = userString[1];
                    String commentAuthorUrlLink = nextComment.select("a").attr("href");//Elements alinks = row.select("a");
                    String commentPermalink = nextComment.select("div.user").select("div.meta-comment").select("a").attr("href");
                    String commentContent = nextComment.select("p").html();
                    int commentContentHash = commentContent.hashCode();
                    String commentDepthClasses[] = nextComment.className().split("-");
                    int commentDepth = Integer.parseInt(commentDepthClasses[commentDepthClasses.length - 1]);
                    String commentIdParts[] = nextComment.id().split("-");
                    int commentInitialId = Integer.parseInt(commentIdParts[commentIdParts.length - 1]);
                    Comment currentComment = new Comment(commentPermalink, commentContent, commentAuthor, commentTimestamp, commentContentHash, commentDepth, commentInitialId, commentAuthorUrlLink);
                    artToCrawl.comments.add(currentComment);
                }
                for (int i = artToCrawl.commentPages - 1; i > 0; i--) {
                    String artToCrawlNext = artToCrawl.url + "&cpage=" + i;
                    artPage = Jsoup.connect(artToCrawlNext).timeout(DEFAULT_TIMEOUT).userAgent("Mozilla").get();
                    commentsList = artPage.select("div#content").select("div#comments").select("ul.comment_list").select("li");
                    ArrayList<Comment> commentList = new ArrayList<>();
                    for (Element nextComment : commentsList) {
                        String[] userString = nextComment.select("div.user").select("div.author").text().split(" | ");
                        String commentTimestamp = userString[0];
                        String commentAuthor = userString[1];
                        String commentAuthorUrlLink = nextComment.select("a").attr("href");//Elements alinks = row.select("a");
                        String commentPermalink = nextComment.select("div.user").select("div.meta-comment").select("a").attr("href");
                        String commentContent = nextComment.select("p").html();
                        int commentContentHash = commentContent.hashCode();
                        String commentDepthClasses[] = nextComment.className().split("-");
                        int commentDepth = Integer.parseInt(commentDepthClasses[commentDepthClasses.length - 1]);
                        String commentIdParts[] = nextComment.id().split("-");
                        int commentInitialId = Integer.parseInt(commentIdParts[commentIdParts.length - 1]);
                        Comment currentComment = new Comment(commentPermalink, commentContent, commentAuthor, commentTimestamp, commentContentHash, commentDepth, commentInitialId, commentAuthorUrlLink);
                        commentList.add(currentComment);
                    }
                    artToCrawl.comments.addAll(0, commentList);
                }
                
                try {
                    // Insert or update comments.
                    DB.InsertComments(artToCrawl.comments, articleDbId);
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            artToCrawl.done = true;
            long end = System.currentTimeMillis();

            if (retryArticles != null) {
                retryArticles[artPos] = -1;
            }
        } catch (SocketTimeoutException sex) {
            Logger.getLogger(ArticleThreadedCrawling.class.getName()).log(Level.SEVERE, sex.getMessage(), sex.getMessage());
            if (retryArticles != null) {
                retryArticles[artPos]++;
                if (Properties.verbose) {
                    System.out.println("Retry #" + retryArticles[artPos] + " for comments of article " + artToCrawl.url);
                }
            } else {
                this.unprocessedArticles.add(article);
            }
        } catch (IOException ex) {
            Logger.getLogger(ArticleThreadedCrawling.class.getName()).log(Level.SEVERE, "*IO*", ex.getMessage());
            if (retryArticles != null) {
                retryArticles[artPos]++;
                if (Properties.verbose) {
                    System.out.println("Retry #" + retryArticles[artPos] + " for comments of article " + artToCrawl.url);
                }
            } else {
                this.unprocessedArticles.add(article);
            }
        }
    }
}
