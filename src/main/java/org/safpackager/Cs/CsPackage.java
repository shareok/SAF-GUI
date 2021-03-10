package org.safpackager.Cs;

import com.csvreader.CsvReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.mozilla.universalchardet.UniversalDetector;
import org.safpackager.OutputXML;
import org.safpackager.Utils;

public class CsPackage {

    private String seperatorRegex = "\\|\\|";
    private static CsPackage cSPackage = null;
    private final String newOutputFilename = "saf-new";
    private final String exiOutputFilename = "saf-existing";
    private Map<String, Map<String, ArrayList>> collectionData = new HashMap<String, Map<String, ArrayList>>();
    private Map<String, Map<String, ArrayList>> newSampleData = new HashMap<String, Map<String, ArrayList>>();
    private Map<String, Map<String, ArrayList>> existingSampleData = new HashMap<String, Map<String, ArrayList>>();
    private List<String> csInfo = new ArrayList();
    private Map<String, Map<String, ArrayList>> zipcodeData = CsValidation.getInstance().getZipcodeData();
    private Map<String, String> stateData = new HashMap<String, String>();
    private Map<String, ArrayList> csPhotos = new TreeMap<String, ArrayList>();
    private String csCsvPath = null;
    private String csPhotoPath = null;
    private String csSavePath = null;
    private String csCollectionPath = null;
    private BufferedWriter mapfileWriter;
    private String lineSeparator = "";

    public CsPackage(String pathToCsv, String pathToPhotos, String pathToSave, String pathToCollection) {
        csCsvPath = pathToCsv;
        csPhotoPath = pathToPhotos;
        csSavePath = pathToSave;
        csCollectionPath = pathToCollection;

        try {
            stateData = CsValidation.getInstance().getStateData();
            getCollectionData();
            getCsPhotos();
        } catch (IOException ex) {
            Logger.getLogger(CsPackage.class.getName()).log(Level.SEVERE, null, ex);
            csInfo.add("Read errors in collection csv");
        }
    }

    public void processMetaPack() throws IOException {
        processMetaPack2(csCsvPath);
    }

    public void processMetaPack2(String csPathToCSV) throws IOException {
        CsvReader csvReader = null;
        csvReader = Utils.openCsv(csPathToCSV);
        csvReader.readHeaders();
  
        while (csvReader.readRecord()) {
            Map<String, ArrayList> items = new TreeMap<String, ArrayList>();
            Map<String, ArrayList> existingItems = new TreeMap<String, ArrayList>();
            int columnCount = 0;
            String sampleId = null;
            String newKey = null;
            String shortState = null;
            String[] values = csvReader.getValues();
            if(values.length > 0) {
                for(String s : csvReader.getHeaders()) {
                    if(!s.equals("")) {
                        s = Utils.convertCsvMetadata(s.toLowerCase());
                        if(s.equals("dwc.npdg.sampleid")) {
                            sampleId = values[columnCount];
                            if(isExisting(sampleId)) {
                                existingItems = collectionData.get(sampleId);
                            } else {
                                newKey = sampleId;
                            }
                        }
                        String item = values[columnCount];
                        if(s.equals("dwc.npdg.homestate")) {
                            shortState = values[columnCount];
                            if(stateData.isEmpty()) {
                                item = "Empty - " + item;
                            } else {
                                item = stateData.get(shortState) + " - " + item;
                            }
                        }
                        add(items, s, item);
                        columnCount++;
                    }
                }                
            }
            
            if(newKey != null) {
                items = updateNewSample(items);
                newSampleData.put(newKey, items);
            } else {
                Map<String, ArrayList> updated = updateExistingSample(existingItems, items);
                existingSampleData.put(sampleId, updated);
            }
        }

        prepareSimpleArchiveFormatDir(csSavePath, newOutputFilename);
        prepareSimpleArchiveFormatDir(csSavePath, exiOutputFilename);

        processMetaBody(newSampleData, newOutputFilename);

        processMetaBody(existingSampleData, exiOutputFilename);

//
//        if(exportToZip) {
//            exportToZip(directoryToSave);
//        }
//
//        printFiles(0);    // print a report of files not used
    }

    public static Charset detectCharsetOfFile(String filePath) throws IOException {
        UniversalDetector detector = new UniversalDetector(null);

        byte[] buf = new byte[4096];
        FileInputStream fis = new FileInputStream(filePath);
        int nread;
        while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
            detector.handleData(buf, 0, nread);
        }
        detector.dataEnd();

        fis.close();

        String charset = detector.getDetectedCharset();
        if(charset == null) {
            charset = "UTF-8";
            System.out.println("Didn't properly detect the charset of file. Setting to UTF-8 as a fallback");
        }
        Charset detectedCharset = Charset.forName(charset);
        System.out.println("Detected input CSV as:" + detectedCharset.displayName());
        return detectedCharset;
    }

    private void getCollectionData() throws IOException {
        CsvReader collectionReader = null;
        collectionReader = Utils.openCsv(csCollectionPath);
        collectionReader.readHeaders();

        while (collectionReader.readRecord()) {
            Map<String, ArrayList> items = new TreeMap<String, ArrayList>();
            int columnCount = 0;
            String sampleId = null;
            String[] values = collectionReader.getValues();
            for(String s : collectionReader.getHeaders()) {
                s = Utils.cleanMetadataField(s);
                
                if(s.contains("dwc.npdg.sampleid".toLowerCase())) {
                    sampleId = values[columnCount];
                }
                add(items, s, values[columnCount]);
                columnCount++;
            }

            collectionData.put(sampleId, items);
        }
    }

    private void prepareSimpleArchiveFormatDir(String directoryToSave, String outputFilename) {
        File directory = new File(directoryToSave + "/" + outputFilename);
        if (directory.exists()) {
            try {
                FileUtils.deleteDirectory(directory);
            } catch (Exception e) {
                e.printStackTrace();
                csInfo.add("Can't delete " + directory.getName());
            }
        }

        directory.mkdir();
    }

    private boolean isExisting(String sampleid) {
        boolean isExisting = false;
        if(sampleid != null && !sampleid.isEmpty()) {
            isExisting = collectionData.get(sampleid) != null;
        }
        return isExisting;
    }

    private Map<String, ArrayList> updateExistingSample(Map<String, ArrayList> existingItems, Map<String, ArrayList> csvItems) {
        Map<String, ArrayList> updated = new TreeMap<String, ArrayList>();
        for(String key : existingItems.keySet()) {
            if(key.split("\\.").length > 1) {
                ArrayList<String> e = existingItems.get(key);
                ArrayList<String> c = csvItems.get(key);
                if(c != null) {
                    updated.put(key, c);
                } else {
                    updated.put(key, e);
                }
            }
        }
        return updated;
    }

    private Map<String, ArrayList> updateNewSample(Map<String, ArrayList> csvItems) {
        Map<String, ArrayList> updated = new TreeMap<String, ArrayList>();
        for(String key : csvItems.keySet()) {
            if(key.split("\\.").length > 1) {
                if(key.equals("dwc.npdg.homezip")) {
                    String zip = (String) csvItems.get(key).get(0);
                    zip = String.valueOf(Integer.parseInt(zip));
                    Map<String, ArrayList> zipData = zipcodeData.get(zip);
                    ArrayList<String> spatial = new ArrayList<String>();
                    try {
                        spatial.add((String) zipData.get("latitude").get(0));
                        spatial.add((String) zipData.get("longitude").get(0));
                        updated.put("dwc.npdg.spatial", spatial);
                    } catch (Exception e) {
                        System.out.print(e);
                    }
                }
                updated.put(key, csvItems.get(key));
            }
        }
        return updated;
    }

    private void getCsPhotos() {
        File[] listOfImages = CsValidation.getInstance().getPhotoList();
        for(File f : listOfImages) {
            String key = f.getName().split("_")[0];
            add(csPhotos, key, f.getName());
        }
    }

    private void processMetaBody(Map<String, Map<String, ArrayList>> sampleData, String outputFilename) {
        int rowCount = 1;
        
        if(outputFilename.equals(exiOutputFilename)) {
            File mapFile = new File(csSavePath + "/" + outputFilename + "/mapfile");
            try { 
                mapfileWriter = new BufferedWriter(new FileWriter(mapFile));
            } catch (IOException ex) {
                Logger.getLogger(CsPackage.class.getName()).log(Level.SEVERE, null, ex);
                csInfo.add("IOException in starting mapfile");
            }
        }

        for(String key : sampleData.keySet()) {
            processMetaRow(sampleData.get(key), outputFilename, rowCount++);
        }

        if(outputFilename.equals(exiOutputFilename)) {
            try {
                mapfileWriter.close();
            } catch (IOException ex) {
                Logger.getLogger(CsPackage.class.getName()).log(Level.SEVERE, null, ex);
                csInfo.add("IOException in closing mapfile");
            }
        }
    }

    private void processMetaRow(Map<String, ArrayList> items, String outputFilename, int rowCount) {
        String currentItemDirectory = makeNewDirectory(outputFilename, rowCount);
        String dcFileName = currentItemDirectory + "/dublin_core.xml";
        File contentsFile = new File(currentItemDirectory + "/contents");
        File handleFile = new File(currentItemDirectory + "/handle");
        ArrayList<String> photoNames = new ArrayList<String>();
        String handle = null;


        OutputXML xmlWriter = new OutputXML(dcFileName);
        xmlWriter.start();
        Map<String, OutputXML> nonDCWriters = new HashMap<String, OutputXML>();

        if(items.get("dwc.npdg.sampleid").get(0) != null) {
            copyPhotos((String)items.get("dwc.npdg.sampleid").get(0), currentItemDirectory);
            photoNames = csPhotos.get((String) items.get("dwc.npdg.sampleid").get(0));

            if(photoNames != null) {
                if(items.get("dwc.npdg.imagestatus").get(0).equals("P")) {
                    items.get("dwc.npdg.imagestatus").clear();
                    items.put("dwc.npdg.imagestatus", new ArrayList<String>());
                    items.get("dwc.npdg.imagestatus").add("Y");
                }
            } else {
                if(items.get("dwc.npdg.imagestatus").get(0).equals("Y")) {
                    items.get("dwc.npdg.imagestatus").clear();
                    items.put("dwc.npdg.imagestatus", new ArrayList<String>());
                    items.get("dwc.npdg.imagestatus").add("P");
                }
            }
        }

        for(String key : items.keySet()) {
            if(key.equals("dc.identifier.uri")) {
                handle = ((String) items.get(key).get(0)).split("hdl.handle.net\\/")[1];
            }
            
            String[] dublinPieces = key.split("\\.");
            if (dublinPieces.length < 2) {
                continue;
            }
            String schema = dublinPieces[0];
            if (schema.contentEquals("dc")) {
                processMetaBodyRowField(key, convertArrayListToString(items.get(key)), xmlWriter);
            } else {
                if (!nonDCWriters.containsKey(schema)) {
                    OutputXML schemaWriter = new OutputXML(currentItemDirectory + File.separator + "metadata_" + schema + ".xml", schema);
                    schemaWriter.start();
                    nonDCWriters.put(schema, schemaWriter);
                }
                processMetaBodyRowField(key, convertArrayListToString(items.get(key)), nonDCWriters.get(schema));
            }

        }

        try {
            BufferedWriter contentsWriter = new BufferedWriter(new FileWriter(contentsFile));
            String contentsRow = "";
            if(photoNames != null) {
                for(String pName : photoNames) {
                    contentsRow = contentsRow.concat(pName + "\t" + "bundle:ORIGINAL");
                    contentsRow = contentsRow.concat("\r\n");
                }                
            }
            contentsWriter.append(contentsRow);
            contentsWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(CsPackage.class.getName()).log(Level.SEVERE, null, ex);
            csInfo.add("IOException in contents file");
        }

        if(handle != null) {
            try {
                mapfileWriter.append(lineSeparator);
                lineSeparator = System.getProperty("line.separator");
                mapfileWriter.append(handle);
            } catch (IOException ex) {
                Logger.getLogger(CsPackage.class.getName()).log(Level.SEVERE, null, ex);
                csInfo.add("IOException in writing mapfile");
            }

            try {
                BufferedWriter handleWriter = new BufferedWriter(new FileWriter(handleFile));
                handleWriter.append(handle);
                handleWriter.newLine();
                handleWriter.close();
            } catch (IOException ex) {
                Logger.getLogger(CsPackage.class.getName()).log(Level.SEVERE, null, ex);
                csInfo.add("IOException in writing handle file");
            }
        }

        xmlWriter.end();
        for (String key : nonDCWriters.keySet()) {
            nonDCWriters.get(key).end();
        }

    }

    private void copyPhotos(String key, String currentItemDirectory) {
        ArrayList<String> photos = csPhotos.get(key);
        if(photos != null) {
            for(String p : photos) {
                String photoPath = csPhotoPath + "/" + p;
                try {
                    FileUtils.copyFileToDirectory(new File(photoPath), new File(currentItemDirectory));
                } catch (IOException ex) {
                    Logger.getLogger(CsPackage.class.getName()).log(Level.SEVERE, null, ex);
                    csInfo.add("Error in copying photo " + p);
                }
            }
        }
    }

    private String makeNewDirectory(String outputFilename, int itemNumber) {
        File newDirectory = new File(csSavePath + "/" + outputFilename + "/" + itemNumber);
        newDirectory.mkdir();
        return newDirectory.getAbsolutePath();
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

    private Map<String, ArrayList> add(Map<String, ArrayList> items, String key, String value)
    {
        if (items.get(key) == null)
        {
            items.put(key, new ArrayList<String>());
        }
        if (value != null && !value.isEmpty())
        {
            items.get(key).add(value);
        }
        return items;
    }

    public List<String> getCsInfo() {
        if(csInfo.size() == 0) {
            csInfo.add("SAF packages were saved in " + csSavePath);
        }
        return csInfo;
    }
}
