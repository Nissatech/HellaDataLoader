package com.nissatech.helladataloader;

import au.com.bytecode.opencsv.CSVReader;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.nissatech.helladataloader.translation.Translator;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.poi.hssf.util.CellReference;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

/**
 *
 * @author aleksandar
 */
public class HellaScrapLoader extends HellaLoader
{

    public HellaScrapLoader(String file, ITimeRounder rounder)
    {
        super(file, rounder);
    }

    @Override
    public void load() throws IOException
    {

        FileInputStream fileInputStream = new FileInputStream(new File(file));
        XSSFWorkbook workbook = new XSSFWorkbook(fileInputStream);
        FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

        DateTimeFormatter fmt = DateTimeFormat.forPattern("dd,MM,yyyy");
        for (int sheetNum = 0; sheetNum < workbook.getNumberOfSheets(); sheetNum++)
        {
            try
            {
                XSSFSheet sheet = workbook.getSheetAt(sheetNum);
                if (sheet.getSheetName().startsWith("P"))
                {

                    CellReference cellReference = new CellReference("D61");
                    XSSFRow row = sheet.getRow(cellReference.getRow());
                    XSSFCell cell = row.getCell(cellReference.getCol());

                    double scrapPct;
                    DateTime parsedTime;
                    if (cell != null)
                    {
                        CellValue cellValue = evaluator.evaluate(cell);
                        scrapPct = cellValue.getNumberValue();

                        cellReference = new CellReference("C3");
                        row = sheet.getRow(cellReference.getRow());
                        cell = row.getCell(cellReference.getCol());
                        if (cell != null)
                        {
                            cellValue = evaluator.evaluate(cell);
                            String cellAsString = cellValue.getStringValue();
                            if (cellAsString != null)
                            {
                                parsedTime = fmt.parseDateTime(cellAsString);
                                parsedTime = parsedTime.plusHours(23).plusMinutes(59);

                                Map<String, String> map = new HashMap<>();
                                map.put("scrap", String.valueOf(scrapPct * 100));
                                addToDB("scrap", rounder.round(parsedTime.getMillis()), parsedTime.getMillis(), map);
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
            }

        }
    }

}
