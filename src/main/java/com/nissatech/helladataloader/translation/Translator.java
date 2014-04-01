package com.nissatech.helladataloader.translation;

import au.com.bytecode.opencsv.CSVReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author aleksandar
 */
public class Translator
{
    Map<String,String> dictionary;

    public Translator(String dictionaryCsv) throws UnsupportedEncodingException, FileNotFoundException, IOException
    {
        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(dictionaryCsv), "UTF-8"));
        String[] line;
        this.dictionary = new HashMap<>();
        while((line = reader.readNext())!=null)
        {
            dictionary.put(line[0], line[1]);
        }
    }
    
    public String translate(String word)
    {
        if(dictionary.containsKey(word))
            return dictionary.get(word);
        else return word;
    }
}
