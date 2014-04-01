package com.nissatech.helladataloader;

import au.com.bytecode.opencsv.CSVReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

/**
 *
 * @author aleksandar
 */
public class HellaCSVLoader extends HellaLoader
{

    public HellaCSVLoader(String file, ITimeRounder rounder)
    {
        super(file, rounder);
    }

    @Override
    public void load() throws IOException
    {

        CSVReader csvReader = new CSVReader(new InputStreamReader(new FileInputStream(file), "UTF-16"));
        //System.out.println("File :" + file.toString());
        String[] line = csvReader.readNext();
        String[] line2 = csvReader.readNext();
        String[] line3 = csvReader.readNext();
        List<String> zipped = zip(line, line2, line3);
        
        
        DateTimeFormatter fmt = DateTimeFormat.forPattern("dd.MM.yy HH:mm:ss");
        
        while((line = csvReader.readNext()) != null)
        {
            try
            {
            HashMap<String, String> map = new HashMap<>();
            String date = line[0] + " " + line[1];
            DateTime parsedDate = fmt.parseDateTime(date);
            for(int i=0; i < line.length; i ++)
            {
                map.put(zipped.get(i), line[i]);
            }
            addToDB("sensors", rounder.round(parsedDate.getMillis()),parsedDate.getMillis(), map);
            }
            catch(Exception ex)
            {
                LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
            }
             
        }

    }

    private List<String> zip(String[] left, String[] mid, String[] right)
    {
        if (left.length != right.length)
        {
            return null;
        }
         List<String> zipped = new ArrayList<>(left.length);
        if (right != null)
        {
            for (int i = 0; i < left.length; i++)
            {
                zipped.add(left[i] + "|" + mid[i] + "|" + right[i]);
            }
        }
        else
        {
            zipped = new ArrayList<>(left.length);
            for (int i = 0; i < left.length; i++)
            {
                zipped.add(left[i] + "|" + mid[i]);
            }
        }

        return zipped;
    }

}
