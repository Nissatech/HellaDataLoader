package com.nissatech.helladataloader;

import au.com.bytecode.opencsv.CSVReader;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.nissatech.helladataloader.translation.Translator;
import java.io.FileInputStream;


import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author aleksandar
 */
public class HellaEventsLoader extends HellaLoader
{

    public HellaEventsLoader(String file, ITimeRounder rounder)
    {
        super(file, rounder);
    }

    @Override
    public void load() throws IOException
    {

        Translator t = new Translator("/media/0D0C19350D0C1935/Projects/ProaSense/Hella data/Translations/KM1DOGODKI.XLS.txt");
        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(file), "UTF-16"),'\t');
        List<String> header = Arrays.asList(reader.readNext());

        DateTimeFormatter fmt = DateTimeFormat.forPattern("dd.MM.yy HH:mm:ss");
        String[] line;
        while ((line = reader.readNext()) != null)
        {
            Map<String,String> map = new HashMap<>();           
            DateTime time = fmt.parseDateTime(line[0]+" " + line[1]);
            long rounded = rounder.round(time.getMillis());
            map.put("L", line[2]);
            map.put("cycle", line[3]);
            map.put("step", line[4]);
            map.put("stepText", t.translate(line[5]));
            map.put("explanation", t.translate(line[6]));
            
            if(line.length >= 8)
                map.put("old value", t.translate(line[7]));
            else
                map.put("old value", "");
            
            if(line.length >= 9)
                map.put("new value", t.translate(line[8]));
            else 
                map.put("new value", "");
            addToDB("events", rounded, time.getMillis(), map);
                    
        }

    }

}
