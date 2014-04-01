package com.nissatech.helladataloader;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author aleksandar
 */
public class App
{

    public static final void main(String[] args)
    {
        HellaLoader loader = null, loader1 = null, loader2 = null;
        try
        {
            /**
             * IMPORTANT!
             * BEFORE LOADING THE EVENTS AND SENSOR CSV FILES PLEASE REMOVE THE HEADER INFORMATION WITH THE EXPORT INFORMATION, SIGNATURE AND SUCH.
             */
            
            loader = new HellaEventsLoader("/media/0D0C19350D0C1935/Projects/ProaSense/Hella data/ROKFEBRUARDOGODKI.XLS - 2 event reports_short.XLS", new Rounder60min());
            
            loader1 = new HellaScrapLoader("/media/0D0C19350D0C1935/Projects/ProaSense/Hella data/Pareto Leč KAS 1 i-KAS mesečna nova - scrap rates .xlsx", new Rounder60min());
            
            loader2 = new HellaCSVLoader("/media/0D0C19350D0C1935/Projects/ProaSense/Hella data/ROKFEBRUARCIKLI_short.csv", new Rounder60min());
            
            loader.load();
            loader1.load();
            loader2.load();


        }
        catch (IOException ex)
        {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally
        {
            if(loader!= null)   loader.close();
            if(loader1!= null)  loader1.close();
            if(loader2!= null)  loader2.close();
        }
    }
}
