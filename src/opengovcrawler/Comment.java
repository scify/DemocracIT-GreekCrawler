package opengovcrawler;

/**
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class Comment implements Comparable {

    public String permalink;
    public String content;
    public String author;
    public String timestamp;
    public int contentHash;
    public Comment[] comments;
    public int depth;
    public int initialId;
    
    /**
     * Comment constructor.
     *
     * @param permalink - The comment's permalink url
     * @param content - The comment's content
     * @param author - The comment's author name
     * @param timestamp - The comment's timestamp
     * @param contentHash - The comment's text md5hash
     * @param depth - The comment's depth in the opengov order
     * @param initialId - The comment's initial id in the opengov comment list
     */
    public Comment(String permalink, String content, String author, String timestamp, int contentHash, int depth, int initialId) {
        this.permalink = permalink;
        this.content = content;
        this.author = author;
        this.timestamp = timestamp;
        this.contentHash = contentHash;
        this.depth = depth;
        this.initialId = initialId;
    }


    public int compareTo(Object o) {
        return 0;
//        Article a = (Article) o;
//        if (a.pubdate.before(pubdate)) {
//            return 1;
//        } else if (a.pubdate.after(pubdate)) {
//            return -1;
//        } else {
//            return 0;
//        }
    }
}
