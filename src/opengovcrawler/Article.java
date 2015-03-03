package opengovcrawler;

import java.util.ArrayList;

/**
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class Article {

    public String url;
    public String title;
    public String content;
    public Boolean hasComments;
    public int numOfComments;
    public String commentsUrl;
    public ArrayList<Comment> comments;
    public int commentPages;
    public Boolean done;
    public int order;
    
    /**
     * Article constructor.
     *
     * @param url - The article's page url
     * @param title - The article's title
     * @param hasComments - Whether article has comments or not
     * @param numOfComments - The article's number of comments
     * @param commentsUrl - The article's comment page url
     * @param commentPages - The article's number of comment pages
     * @param order - The article's order in the consultation's article list
     */
    public Article(String url, String title, Boolean hasComments, int numOfComments, String commentsUrl, int commentPages, int order) { //String content, 
        this.done = false;
        this.url = url;
        this.title = title;
        this.hasComments = hasComments;
        this.numOfComments = numOfComments;
        this.commentsUrl = commentsUrl;
        this.comments = new ArrayList<>();
        this.commentPages = commentPages;
        this.order = order;
    }
}