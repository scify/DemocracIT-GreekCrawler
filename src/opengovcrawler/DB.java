/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

//import com.mysql.jdbc.Statement;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;
import static opengovcrawler.GetMinistries.configFile;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Performs all the database transactions.
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class DB {

    static Connection connection = null;
    static Locale locale = new Locale("el-GR");
    static SimpleDateFormat formatter = new SimpleDateFormat("dd MM yyyy, HH:mm", locale);

    /**
     * Initiates the database connection.
     *
     * @throws java.sql.SQLException
     * @throws java.io.FileNotFoundException
     * @throws java.io.IOException
     */
    public static void init() throws SQLException, FileNotFoundException, IOException {
        if (connection == null) {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
                e.printStackTrace();
                return;
            }

            try {
                BufferedReader br = null;
                String line = "";
                String splitBy = "=";
                String ip_address = null;
                String user = null;
                String pass = null;
                try {
                    br = new BufferedReader(new FileReader(configFile));
                    while ((line = br.readLine()) != null) {
                        if (line.startsWith("IP_ADDRESS")) {
                            String[] lineParts = line.split(splitBy, 2);
                            ip_address = lineParts[1];
                        } else if (line.startsWith("USERNAME")) {
                            String[] lineParts = line.split(splitBy, 2);
                            user = lineParts[1];
                        } else if (line.startsWith("PASSWORD")) {
                            String[] lineParts = line.split(splitBy, 2);
                            pass = lineParts[1];
                        }
                    }
                } catch (FileNotFoundException e) {
                }
                String DB_url = "jdbc:postgresql://" + ip_address;
                connection = DriverManager.getConnection(DB_url, user, pass);
            } catch (SQLException e) {
                System.out.println("Connection Failed! Check output console.");
                e.printStackTrace();
                return;
            }
        }
    }

    public static void close() throws SQLException {
        if (connection != null) {
            connection.close();
            Connection connection = null;
        }
    }

    /**
     * Returns the db_status of a consultation from the db.
     *
     * @param curl - The consultation's url
     * @return - The consultation's db status
     * @throws java.sql.SQLException
     */
    public static String GetConsultationStatus(String curl) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT completed FROM consultation WHERE consultation_url  = '" + curl + "';");
        int db_status = 0;
        if (rs.next()) {
            db_status = rs.getInt(1);
        }
        if (db_status == 2) {
            return "blue";
        } else if (db_status == 1) {
            return "red";
        } else {
            return "green";
        }
    }

    /**
     * Returns the ID of a given consultation if already on the DB else returns
     * -1.
     *
     * @param curl - The consultation's url
     * @return - The consultation's db id
     * @throws java.sql.SQLException
     */
    public static int GetConsultationId(String curl) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID FROM Consultation WHERE consultation_url = '" + curl + "';");
        int id = -1;
        if (rs.next()) {
            id = rs.getInt(1);
        }
        return id;
    }

    /**
     * Inserts organizations into DB.
     *
     * @param ministry - The ministry object
     * @param url - The url of the ministry
     * @throws java.sql.SQLException
     */
    public static void InsertOrganization(Object ministry, String url) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID FROM ORGANIZATION_LKP WHERE title = '" + ministry + "';");
        int id = -1;
        if (rs.next()) {
            id = rs.getInt(1);
        } else {
            stmt.execute("INSERT INTO ORGANIZATION_LKP (title, url_initial) VALUES ('" + ministry + "','" + url + "');");
        }
    }

    /**
     * Updates the urls of the organizations into DB.
     *
     * @param x - The ministry object
     * @param url - The collapsed url of the ministry
     * @throws java.sql.SQLException
     */
    static void UpdateOrganizationUrls(Object x, String url) throws SQLException {
        String updateOrganizationUrlSql = "UPDATE ORGANIZATION_LKP SET "
                + "url_collapsed = ?"
                + "WHERE title = ?";
        PreparedStatement prepUpdUrlsSt = connection.prepareStatement(updateOrganizationUrlSql);
        prepUpdUrlsSt.setString(1, url);
        prepUpdUrlsSt.setString(2, (String) x);
        prepUpdUrlsSt.executeUpdate();
        prepUpdUrlsSt.close();
    }

    /**
     * Returns the ID of a given Organization if already on the DB else Inserts
     * the new Organization into DB.
     *
     * @param ministry - The name of the ministry
     * @return - The DB id of any given ministry
     * @throws java.sql.SQLException
     */
    public static int GetOrganizationId(Object ministry) throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT ID FROM ORGANIZATION_LKP WHERE TITLE = '" + ministry + "';");
        int id = -1;
        if (rs.next()) {
            id = rs.getInt(1);
        }
        return id;
    }

    /**
     * Get a string date and replace the months into the appropriate format.
     *
     * @param date - A date into "Day Month Year" format.
     * @return - A new java.sql.Timestamp format of this date
     * @throws java.text.ParseException
     */
    public static Timestamp ConvertDateMonth(String date) throws ParseException {
        String[] dateTokens = date.split(" ");
        switch (dateTokens[1]) {
            case "Ιανουαρίου":
                dateTokens[1] = "01";
                break;
            case "Φεβρουαρίου":
                dateTokens[1] = "02";
                break;
            case "Μαρτίου":
                dateTokens[1] = "03";
                break;
            case "Απριλίου":
                dateTokens[1] = "04";
                break;
            case "Μαΐου":
                dateTokens[1] = "05";
                break;
            case "Ιουνίου":
                dateTokens[1] = "06";
                break;
            case "Ιουλίου":
                dateTokens[1] = "07";
                break;
            case "Αυγούστου":
                dateTokens[1] = "08";
                break;
            case "Σεπτεμβρίου":
                dateTokens[1] = "09";
                break;
            case "Οκτωβρίου":
                dateTokens[1] = "10";
                break;
            case "Νοεμβρίου":
                dateTokens[1] = "11";
                break;
            case "Δεκεμβρίου":
                dateTokens[1] = "12";
                break;
            case "/":
                dateTokens[1] = "/";
                break;
        }
        String newDate = null;
        if (dateTokens.length < 4) {
            newDate = "00 00 0000, 00:00";
        } else {
            newDate = dateTokens[0] + " " + dateTokens[1] + " " + dateTokens[2] + " " + dateTokens[3];
        }
        java.util.Date curDate = formatter.parse(newDate);
        return new Timestamp(curDate.getTime());
    }

    // Get an html string and escape the html characters
    public static String EscapeHtml(String htmlText) {
        String escapedText = StringEscapeUtils.unescapeHtml4(htmlText);//escapeHtml(htmlText);
        return escapedText;
    }

    /**
     * Insert consultations into DB.
     *
     * @param currentCons - The consultation to be inserted
     * @param orgId - the ministry id that the consultation belongs to
     * @param numOfArticles - The number of articles of the current consultation
     * @return - The database ID for the inserted consultation
     * @throws java.sql.SQLException
     */
    public static int InsertNewConsultation(Consultation currentCons, int orgId, int numOfArticles) throws SQLException {
        Timestamp startDate = null;
        Timestamp endDate = null;
        try {
            // Convert Strings to Datetimes
            startDate = ConvertDateMonth(currentCons.startDate);
            endDate = ConvertDateMonth(currentCons.endDate);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        String insertConsultationSql = "INSERT INTO consultation"
                + "(start_date, end_date, title, short_description, organization_id, consultation_url, completed, completed_text, report_text, report_url, num_of_articles) VALUES"
                + "(?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertConsultationSql, PreparedStatement.RETURN_GENERATED_KEYS);
        preparedStatement.setTimestamp(1, startDate);
        preparedStatement.setTimestamp(2, endDate);
        preparedStatement.setString(3, currentCons.title);
        preparedStatement.setString(4, currentCons.bodyText);
        preparedStatement.setInt(5, orgId);
        preparedStatement.setString(6, currentCons.url);
        preparedStatement.setInt(7, 0);
        preparedStatement.setString(8, currentCons.completed_text);
        preparedStatement.setString(9, currentCons.report_text);
        preparedStatement.setString(10, currentCons.report_url);
        preparedStatement.setInt(11, numOfArticles);
        preparedStatement.executeUpdate();
        ResultSet rs = preparedStatement.getGeneratedKeys();
        int conIdAfterIns = -1;
        if (rs.next()) {
            conIdAfterIns = rs.getInt(1);
        }
        preparedStatement.close();
        return conIdAfterIns;
    }

    /**
     * Update consultations into DB, adding report texts etc.
     *
     * @param currentCons - The consultation to be updated.
     * @param consID - The consultation's id
     * @param orgId - the ministry id that the consultation belongs to
     * @param numOfArticles - The number of articles of the current consultation
     * @throws java.sql.SQLException
     */
    public static void UpdateConsultation(Consultation currentCons, int orgId, int consID, int numOfArticles) throws SQLException {
        Timestamp startDate = null;
        Timestamp endDate = null;
        int completed;
        if (currentCons.completed.equals("blue")) {
            completed = 2;
        } else if (currentCons.completed.equals("red")) {
            completed = 1;
        } else {
            completed = 0;
        }
        try {
            // Convert Strings to Datetimes
            startDate = ConvertDateMonth(currentCons.startDate);
            endDate = ConvertDateMonth(currentCons.endDate);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        String updateConsultationSql = "UPDATE consultation SET "
                + "start_date = ?, end_date = ?, title = ?, short_description = ?, organization_id = ?, "
                + "consultation_url = ?, completed = ?, completed_text = ?, report_text = ?, report_url = ?, num_of_articles = ?  "
                + "WHERE id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(updateConsultationSql);
        preparedStatement.setTimestamp(1, startDate);
        preparedStatement.setTimestamp(2, endDate);
        preparedStatement.setString(3, currentCons.title);
        preparedStatement.setString(4, currentCons.bodyText);
        preparedStatement.setInt(5, orgId);
        preparedStatement.setString(6, currentCons.url);
        preparedStatement.setInt(7, completed);
        preparedStatement.setString(8, currentCons.completed_text);
        preparedStatement.setString(9, currentCons.report_text);
        preparedStatement.setString(10, currentCons.report_url);
        preparedStatement.setInt(11, numOfArticles);
        preparedStatement.setInt(12, consID);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    /**
     * Update the consultation status (from red2blue)
     *
     * @param currentCons - The consultation to be updated.
     * @param consID - The consultation's id
     * @param orgId - the ministry id that the consultation belongs to
     * @throws java.sql.SQLException
     */
    static void UpdateConsultationStatus(Consultation currentCons, int orgId, int consID) throws SQLException {
        int completed;
        switch (currentCons.completed) {
            case "blue":
                completed = 2;
                break;
            case "red":
                completed = 1;
                break;
            default:
                completed = 0;
                break;
        }
        String updateConsultationStatusSql = "UPDATE consultation SET "
                + "completed = ?, report_text = ?, report_url = ? "
                + "WHERE id = ?";
        PreparedStatement prepUpdStatusSt = connection.prepareStatement(updateConsultationStatusSql);
        prepUpdStatusSt.setInt(1, completed);
        prepUpdStatusSt.setString(2, currentCons.report_text);
        prepUpdStatusSt.setString(3, currentCons.report_url);
        prepUpdStatusSt.setInt(4, consID);
        prepUpdStatusSt.executeUpdate();
        prepUpdStatusSt.close();
    }

    /**
     * Insert articles into DB
     *
     * @param a - The article to be stored
     * @param consID - The consultation's id the the article refers to
     * @return - Returns the article id
     * @throws java.sql.SQLException
     */
    public static int InsertArticles(Article a, int consID) throws SQLException {
        // First check if article is already into db. (Consultation might be green and we crawl for new comments,
        // so articles might already exist in the db)
        String selectArticleSql = "SELECT id FROM articles WHERE consultation_id = ? AND title = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(selectArticleSql);
        preparedStatement.setInt(1, consID);
        preparedStatement.setString(2, a.title);
        ResultSet result = preparedStatement.executeQuery();
        int articleID = -1;
        if (result.next()) {
            articleID = result.getInt(1);
        } else {
            String insertArticleSql = "INSERT INTO articles (consultation_id, title, body, art_order, comment_num) VALUES (?,?,?,?,?)";
            PreparedStatement prepInsertStatement = connection.prepareStatement(insertArticleSql, Statement.RETURN_GENERATED_KEYS);
            prepInsertStatement.setInt(1, consID);
            prepInsertStatement.setString(2, a.title);
            prepInsertStatement.setString(3, a.content);
            prepInsertStatement.setInt(4, a.order);
            prepInsertStatement.setInt(5, a.numOfComments);
            prepInsertStatement.executeUpdate();
            ResultSet rsq = prepInsertStatement.getGeneratedKeys();
            if (rsq.next()) {
                articleID = rsq.getInt(1);
            }
            prepInsertStatement.close();
        }
        preparedStatement.close();
        return articleID;
    }

    /**
     * Insert comments into DB
     * Also, it inserts username and initialId into comment_opengov table
     *
     * @param articleDbId - The id of the article that the comments refer to
     * @param comments - The arrayList of comment
     * @throws java.sql.SQLException
     */
    public static void InsertComments(ArrayList<Comment> comments, int articleDbId) throws SQLException {
        String insertCommentSql = "INSERT INTO comments (url_source, article_id, comment, date_added, revision, depth, source_type_id)"
                + "VALUES (?,?,?,?,?,?,?)";
        PreparedStatement prepInsertComStatement = connection.prepareStatement(insertCommentSql, Statement.RETURN_GENERATED_KEYS);
        Statement stmnt = null;
        for (Comment currentComment : comments) {
            String selectCommentSql = "SELECT comment FROM comments WHERE url_source = ? AND article_id = ?";
            PreparedStatement prepSelectComStatement = connection.prepareStatement(selectCommentSql);
            prepSelectComStatement.setString(1, currentComment.permalink);
            prepSelectComStatement.setInt(2, articleDbId);
            ResultSet rs = prepSelectComStatement.executeQuery();
            int insertedCommentKeyId = -1;
            if (rs.next()) {
                String comText = rs.getString("comment");
                if (currentComment.contentHash != comText.hashCode()) {
                    // Then comment has been changed so
                    // we insert it as in the DB as well, as revision-2
                    Timestamp comTimestamp = null;
                    try {
                        comTimestamp = ConvertDateMonth(currentComment.timestamp);
                    } catch (ParseException ex) {
                        ex.printStackTrace();
                    }
                    int curCommentRevision = rs.getInt("revision");
                    curCommentRevision++;
                    prepInsertComStatement.setString(1, currentComment.permalink);
                    prepInsertComStatement.setInt(2, articleDbId);
                    prepInsertComStatement.setString(3, currentComment.content);
                    prepInsertComStatement.setTimestamp(4, comTimestamp);
                    prepInsertComStatement.setInt(5, curCommentRevision);
                    prepInsertComStatement.setInt(6, currentComment.depth);
                    prepInsertComStatement.setInt(7, 2);
                    prepInsertComStatement.executeUpdate();
                    ResultSet rsq = prepInsertComStatement.getGeneratedKeys();
                    if (rsq.next()) {
                        insertedCommentKeyId = rsq.getInt(1);
                    }
                    prepInsertComStatement.close();
//                    prepInsertComStatement.addBatch();
                    ConsultationThreadedCrawling.newComments++;
                    String insertIntoCommentOpengov = "INSERT INTO comment_opengov"
                            + "(opengovId, fullname, id) " + "VALUES"
                            + "(" + currentComment.initialId + ",'" + currentComment.author + "'," + insertedCommentKeyId + ")";
                    stmnt = connection.createStatement();
                    stmnt.executeUpdate(insertIntoCommentOpengov);
                    stmnt.close();
                }
            } else {
                Timestamp comTimestamp = null;
                try {
                    comTimestamp = ConvertDateMonth(currentComment.timestamp);
                } catch (ParseException ex) {
                    ex.printStackTrace();
                }
                prepInsertComStatement.setString(1, currentComment.permalink);
                prepInsertComStatement.setInt(2, articleDbId);
                prepInsertComStatement.setString(3, currentComment.content);
                prepInsertComStatement.setTimestamp(4, comTimestamp);
                prepInsertComStatement.setInt(5, 1);
                prepInsertComStatement.setInt(6, currentComment.depth);
                prepInsertComStatement.setInt(7, 2);
                prepInsertComStatement.executeUpdate();
                ResultSet rsq = prepInsertComStatement.getGeneratedKeys();
                if (rsq.next()) {
                    insertedCommentKeyId = rsq.getInt(1);
                }
                prepInsertComStatement.close();
//                prepInsertComStatement.addBatch();
                ConsultationThreadedCrawling.newComments++;
                String insertIntoCommentOpengov = "INSERT INTO comment_opengov"
                        + "(opengovId, fullname, id) " + "VALUES"
                        + "(" + currentComment.initialId + ",'" + currentComment.author + "'," + insertedCommentKeyId + ")";
                stmnt = connection.createStatement();
                stmnt.executeUpdate(insertIntoCommentOpengov);
                stmnt.close();
            }
            prepSelectComStatement.close();
        }
//        prepInsertComStatement.executeBatch();
//        prepInsertComStatement.close();
    }

    /**
     * Insert consultation's relevant material (pdfs) into DB
     *
     * @param curPdf - The ReferencedPdf object to be stored
     * @param consID - The consultation id of this pdf
     * @throws java.sql.SQLException
     */
    public static void InsertRelevantMaterial(ReferencedPdf curPdf, int consID) throws SQLException {
        String selectRelevantMaterialSql = "SELECT id FROM relevant_mat WHERE consultation_id = ? AND url_source = ?";
        PreparedStatement prepSelectStatement = connection.prepareStatement(selectRelevantMaterialSql);
        prepSelectStatement.setInt(1, consID);
        prepSelectStatement.setString(2, curPdf.refMaterialFakeUrl);
        ResultSet result = prepSelectStatement.executeQuery();
        int pdfID = -1;
        if (result.next()) {
            pdfID = result.getInt(1);
        } else {
            String insertPdfSql = "INSERT INTO relevant_mat (title, url_source, consultation_id, actual_pdf_url, md5_hash, relative_path) VALUES (?,?,?,?,?,?)";
            PreparedStatement prepInsertPdfStatement = connection.prepareStatement(insertPdfSql);
            prepInsertPdfStatement.setString(1, curPdf.refMaterialTitle);
            prepInsertPdfStatement.setString(2, curPdf.refMaterialFakeUrl);
            prepInsertPdfStatement.setInt(3, consID);
            prepInsertPdfStatement.setString(4, curPdf.refMaterialActualUrl);
            prepInsertPdfStatement.setString(5, curPdf.md5hash);
            prepInsertPdfStatement.setString(6, curPdf.relativePath);
            prepInsertPdfStatement.executeUpdate();
            prepInsertPdfStatement.close();
        }
        prepSelectStatement.close();
    }

    /**
     * Logs unprocessed articles into DB
     *
     * @param failStartTime - Start time of unsuccessfully crawled consultation
     * @param failEndTime - End time of unsuccessfully crawled consultation
     * @param unFetchedArt - The url of the unsuccessfully crawled consultation
     * @param activity_id - The activity id of the unsuccessfully crawled
     * consultation
     * @param status_code - The status id of the unsuccessfully crawled
     * consultation
     * @throws java.sql.SQLException
     */
    public static void LogUnprocessedArticles(long failStartTime, long failEndTime, String unFetchedArt, int activity_id, int status_code) throws SQLException {
        String insertLogSql = "INSERT INTO log.activity_steps (activity_id, status_id, start_date, end_date, message, type_id) VALUES (?,?,?,?,?,?)";
        PreparedStatement prepInsertLogStatement = connection.prepareStatement(insertLogSql);
        prepInsertLogStatement.setInt(1, activity_id);
        prepInsertLogStatement.setInt(2, status_code);
        prepInsertLogStatement.setTimestamp(3, new java.sql.Timestamp(failStartTime));
        prepInsertLogStatement.setTimestamp(4, new java.sql.Timestamp(failEndTime));
        prepInsertLogStatement.setString(5, unFetchedArt);
        prepInsertLogStatement.setInt(6, 3);
        prepInsertLogStatement.executeUpdate();
        prepInsertLogStatement.close();
    }

    /**
     * Logs unprocessed consultations into DB
     *
     * @param failStartTime - Start time of unsuccessfully crawled consultation
     * @param failEndTime - End time of unsuccessfully crawled consultation
     * @param url - The url of the unsuccessfully crawled consultation
     * @param activity_id - The activity id of the unsuccessfully crawled
     * consultation
     * @param status_id - The status id of the unsuccessfully crawled
     * consultation
     * @throws java.sql.SQLException
     */
    public static void LogUnprocessedConsultations(long failStartTime, long failEndTime, String url, int activity_id, int status_id) throws SQLException {
        String insertLogSql = "INSERT INTO log.activity_steps (activity_id, status_id, start_date, end_date, message, type_id) VALUES (?,?,?,?,?,?)";
        PreparedStatement prepInsertLogStatement = connection.prepareStatement(insertLogSql);
        prepInsertLogStatement.setInt(1, activity_id);
        prepInsertLogStatement.setInt(2, status_id);
        prepInsertLogStatement.setTimestamp(3, new java.sql.Timestamp(failStartTime));
        prepInsertLogStatement.setTimestamp(4, new java.sql.Timestamp(failEndTime));
        prepInsertLogStatement.setString(5, url);
        prepInsertLogStatement.setInt(6, 2);
        prepInsertLogStatement.executeUpdate();
        prepInsertLogStatement.close();
    }

    /**
     * Logs processed consultations into DB
     *
     * @param successStartTime - Start time of successfully crawled consultation
     * @param successEndTime - End time of successfully crawled consultation
     * @param url - The url of the crawled consultation
     * @param activity_id - The activity id of the crawled consultation
     * @param status_id - The status id of the crawled consultation
     * @throws java.sql.SQLException
     */
    public static void LogProcessedConsultation(long successStartTime, long successEndTime, String url, int activity_id, int status_id) throws SQLException {
        String insertLogSql = "INSERT INTO log.activity_steps (activity_id, status_id, start_date, end_date, message, type_id) VALUES (?,?,?,?,?,?)";
        PreparedStatement prepInsertLogStatement = connection.prepareStatement(insertLogSql);
        prepInsertLogStatement.setInt(1, activity_id);
        prepInsertLogStatement.setInt(2, status_id);
        prepInsertLogStatement.setTimestamp(3, new java.sql.Timestamp(successStartTime));
        prepInsertLogStatement.setTimestamp(4, new java.sql.Timestamp(successEndTime));
        prepInsertLogStatement.setString(5, url);
        prepInsertLogStatement.setInt(6, 2);
        prepInsertLogStatement.executeUpdate();
        prepInsertLogStatement.close();
    }

    /**
     * Starts the crawler's activity log
     *
     * @param startTime - The start time of the crawling procedure
     * @return - The activity's log id
     * @throws java.sql.SQLException
     */
    public static int LogCrawler(long startTime) throws SQLException {
        String insertLogSql = "INSERT INTO log.activities (module_id, start_date, end_date, status_id, message) VALUES (?,?,?,?,?)";
        PreparedStatement prepLogCrawlStatement = connection.prepareStatement(insertLogSql, Statement.RETURN_GENERATED_KEYS);
        prepLogCrawlStatement.setInt(1, 1);
        prepLogCrawlStatement.setTimestamp(2, new java.sql.Timestamp(startTime));
        prepLogCrawlStatement.setTimestamp(3, null);
        prepLogCrawlStatement.setInt(4, 1);
        prepLogCrawlStatement.setString(5, null);
        prepLogCrawlStatement.executeUpdate();
        ResultSet rsq = prepLogCrawlStatement.getGeneratedKeys();
        int crawlerId = 0;
        if (rsq.next()) {
            crawlerId = rsq.getInt(1);
        }
        prepLogCrawlStatement.close();
        return crawlerId;
    }

    /**
     * Update the crawler's activity log
     *
     * @param endTime
     * @param status_id
     * @param crawlerId
     * @param message
     * @throws java.sql.SQLException
     */
    public static void UpdateLogCrawler(long endTime, int status_id, int crawlerId, String message) throws SQLException {
        String updateCrawlerStatusSql = "UPDATE log.activities SET "
                + "end_date = ?, status_id = ?, message = ?"
                + "WHERE id = ?";
        PreparedStatement prepUpdStatusSt = connection.prepareStatement(updateCrawlerStatusSql);
        prepUpdStatusSt.setTimestamp(1, new java.sql.Timestamp(endTime));
        prepUpdStatusSt.setInt(2, status_id);
        prepUpdStatusSt.setString(3, message);
        prepUpdStatusSt.setInt(4, crawlerId);
        prepUpdStatusSt.executeUpdate();
        prepUpdStatusSt.close();
    }

    /**
     * Checks if a consultation is into a skip list, in order to skip crawling
     *
     * @param consultationPage - The consultation's page url
     * @return - True if consultation exists in skip list
     * @throws java.sql.SQLException
     */
    static boolean CheckSkipList(String consultationPage) throws SQLException {
        boolean exists = false;
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM log.consultation_skip_list WHERE url = '" + consultationPage + "';");
        int id = -1;
        if (rs.next()) {
            id = rs.getInt(1);
            exists = true;
        }
        return exists;
    }
}
