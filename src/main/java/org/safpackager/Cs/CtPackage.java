package org.safpackager.Cs;

import com.csvreader.CsvReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.safpackager.OutputXML;
import org.safpackager.Utils;
import org.safpackager.Utils.IdType;

public class CtPackage {
    private String seperatorRegex = "\\|\\|";
    private String ctCsvPath = null;
    private String ctSavePath = null;
    private String ctCollectionPath = null;
    private String OutputFilename = "saf-taxonomy";
    private Map<String, Map<String, ArrayList>> taxData = new HashMap<String, Map<String, ArrayList>>();
    private Map<String, Map<String, ArrayList>> collectionData;
    private List<String> ctInfo = new ArrayList();

    public CtPackage(String pathToCollection, String pathToCsv, String pathToSave) {
        ctCollectionPath = pathToCollection;
        ctCsvPath = pathToCsv;
        ctSavePath = pathToSave;
        // CtValidation ctValidation = new CtValidation(ctCollectionPath, IdType.INTERNALID);
        CtValidation ctValidation = new CtValidation(ctCollectionPath, IdType.SAMPLEID);
        collectionData = ctValidation.getCollectionDataInstance();
    }

    public void processMetaPack() throws IOException {
        processMetaPack2(ctCsvPath);
    }

    public void processMetaPack2(String ctPathToCSV) throws IOException {
        CsvReader csvReader = null;
        csvReader = Utils.openCsv(ctPathToCSV);
        csvReader.readHeaders();
        int id = 0;
        while (csvReader.readRecord()) {
            Map<String, ArrayList> items = new TreeMap<String, ArrayList>();
            int columnCount = 0;
            String[] values = csvReader.getValues();

            for(String s : csvReader.getHeaders()) {
                s = Utils.convertCsvMetadata(s.toLowerCase());
                String item = values[columnCount];
                if(s.split("\\.").length > 1) {
                    add(items, s, item);
                }
                columnCount++;
            }
            taxData.put(String.valueOf(id), items);
            id++;
        }

        prepareSimpleArchiveFormatDir(ctSavePath, OutputFilename);
        processMetaBody(taxData, OutputFilename);

    }

    private void processMetaBody(Map<String, Map<String, ArrayList>> taxData, String outputFilename) {
        int rowCount = 1;
        for(String key : taxData.keySet()) {
            processMetaRow(taxData.get(key), outputFilename, rowCount++);
        }
    }

    private void processMetaRow(Map<String, ArrayList> items, String outputFilename, int rowCount) {
        String currentItemDirectory = makeNewDirectory(outputFilename, rowCount);
        String dcFileName = currentItemDirectory + "/dublin_core.xml";
        Map<String, ArrayList> values;
        String acceptedKey = null;
        ArrayList<String> acceptedValues = null; 

        OutputXML xmlWriter = new OutputXML(dcFileName);
        xmlWriter.start();
        Map<String, OutputXML> nonDCWriters = new HashMap<String, OutputXML>();

        for(String key : items.keySet()) {
            if(key.equals("dwc.npdg.internalcode")) {
                String internalcode = (String) items.get(key).get(0);
                values = collectionData.get(internalcode);
                acceptedKey = "dc.identifier.uri";
                acceptedValues = values.get("dc.identifier.uri");
            } else if(key.equals("dwc.npdg.sampleid")) {
                String sampleid = (String) items.get(key).get(0);
                values = collectionData.get(sampleid);
                acceptedKey = "dc.identifier.uri";
                acceptedValues = values.get("dc.identifier.uri");
            }
            if(key.equals("dc.relation.wiki")) {
                acceptedKey = key;
                acceptedValues = items.get(key);
            }
            
            if(acceptedKey != null) {
                String[] dublinPieces = acceptedKey.split("\\.");
                if (dublinPieces.length < 2) {
                    continue;
                }
                String schema = dublinPieces[0];
                if (schema.contentEquals("dc")) {
                    processMetaBodyRowField(acceptedKey, convertArrayListToString(acceptedValues), xmlWriter);
                } else {
                    if (!nonDCWriters.containsKey(schema)) {
                        OutputXML schemaWriter = new OutputXML(currentItemDirectory + File.separator + "metadata_" + schema + ".xml", schema);
                        schemaWriter.start();
                        nonDCWriters.put(schema, schemaWriter);
                    }
                    processMetaBodyRowField(acceptedKey, convertArrayListToString(acceptedValues), nonDCWriters.get(schema));
                }                
            }
        }

        xmlWriter.end();
        for (String key : nonDCWriters.keySet()) {
            nonDCWriters.get(key).end();
        }
    }

    private String convertArrayListToString(ArrayList<String> list) {
        StringBuilder sb = new StringBuilder();
        String prefix = "";
        for (String s : list)
        {
            sb.append(prefix);
            prefix = ",";
            sb.append(s);
        }
        
        return sb.toString();
    }


    private void processMetaBodyRowField(String field_header, String field_value, OutputXML xmlWriter) {
        String[] fieldValues = field_value.split(seperatorRegex);
        for (int valueNum = 0; valueNum < fieldValues.length; valueNum++) {
            if (fieldValues[valueNum].trim().length() > 0) {
                xmlWriter.writeOneDC(field_header, fieldValues[valueNum].trim());
            } else {
                continue;
            }
        }
    }

    private void prepareSimpleArchiveFormatDir(String directoryToSave, String outputFilename) {
        File directory = new File(directoryToSave + "/" + outputFilename);
        if (directory.exists()) {
            try {
                FileUtils.deleteDirectory(directory);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        directory.mkdir();
    }

    private String makeNewDirectory(String outputFilename, int itemNumber) {
        File newDirectory = new File(ctSavePath + "/" + outputFilename + "/" + itemNumber);
        newDirectory.mkdir();
        return newDirectory.getAbsolutePath();
    }

    private Map<String, ArrayList> add(Map<String, ArrayList> items, String key, String value)
    {
        if (items.get(key) == null)
        {
            items.put(key, new ArrayList<String>());
        }
        if (value != null)
        {
            items.get(key).add(value);
        }
        return items;
    }
    
    public List<String> getCsInfo() {
        if(ctInfo.size() == 0) {
            ctInfo.add("Taxonomy package was saved in " + ctSavePath);
        }
        return ctInfo;
    }

}
