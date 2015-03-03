/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

import java.util.ArrayList;

/**
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class Consultation {

    public String url;
    public String title;
    public String bodyText;
    public String startDate;
    public String endDate;
    public ArrayList<Article> articles;
    public String completed;
    public String completed_text;
    public String report_text;
    public String report_url;
    
    /**
     * Consultation constructor.
     *
     * @param url - The consultation's url
     * @param title - The consultation's title
     * @param bodyText - The consultation's description text
     * @param startDate - The consultation's start date
     * @param endDate - The consultation's end date
     * @param articles - The consultation's article list
     * @param completed - The completed consultation's status
     * @param completed_text - The completed consultation's description text
     * @param report_text - The completed consultation's report text
     * @param report_url - The completed consultation's url
     */
    public Consultation(String url, String title, String bodyText, String startDate, String endDate, ArrayList articles, String completed, String completed_text, String report_text, String report_url) {
        this.url = url;
        this.title = title;
        this.bodyText = bodyText;
        this.startDate = startDate;
        this.endDate = endDate;
        this.articles = articles;
        this.completed = completed;
        this.completed_text = completed_text;
        this.report_text = report_text;
        this.report_url = report_url;
    }
}
