/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package opengovcrawler;

/**
 *
 * @author Christos Sardianos
 * @version 1.0
 */
public class ConsultationsList {
    public String consultationTitle;
    public String consultationUrl;
    
    @Override
    public String toString() {
        return consultationTitle.toString() + "\t" + consultationUrl.toString();
    }
}
