package org.safpackager.Cs;

import com.csvreader.CsvReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.safpackager.Utils;

public class CtValidation {

    private Map<String, Map<String, ArrayList>> collectionData = new HashMap<String, Map<String, ArrayList>>();
    private String ctCollectionPath = null;
    private static CtValidation validation = null;

    public CtValidation(String pathToCollection) {
        ctCollectionPath = pathToCollection;
        try {
            getCollectionData();
        } catch (IOException ex) {
            Logger.getLogger(CsValidation.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public ArrayList<String> validateCsv(String csvPath) throws IOException {
        ArrayList<String> errors = new ArrayList<String>();
        boolean hasLink = false;

        CsvReader inputCsv = Utils.openCsv(csvPath);
        inputCsv.readHeaders();

        while(inputCsv.readRecord()) {
            int col = 0;
            String[] values = inputCsv.getValues();
            for(String s : inputCsv.getHeaders()) {
                s = s.toLowerCase();
                if(s.contains("Internal ID".toLowerCase())) {
                    String internalId = values[col];
                    if(!hasInternalID(internalId)) {
                        errors.add(internalId);
                    }
                }
                if(s.contains("Link")) {
                    hasLink = true;
                }
                col++;
            }
        }
        
        if(!hasLink) {
            errors.add("The column Link does not exist.");
        }
        
        return errors;
    }


    private void getCollectionData() throws IOException {
        CsvReader collectionReader;
        collectionReader = Utils.openCsv(ctCollectionPath);
        collectionReader.readHeaders();

        while (collectionReader.readRecord()) {
            Map<String, ArrayList> items = new TreeMap<String, ArrayList>();
            int columnCount = 0;
            String internalId = null;
            String[] values = collectionReader.getValues();
            for(String s : collectionReader.getHeaders()) {
                s = Utils.cleanMetadataField(s);
                
                if(s.contains("dwc.npdg.internalcode".toLowerCase())) {
                    internalId = values[columnCount];
                }
                add(items, s, values[columnCount]);
                columnCount++;
            }
            if(internalId != null && !"".equals(internalId)) {
                collectionData.put(internalId, items);
            }
        }
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

    private boolean hasInternalID(String internalid) {
        Map<String, ArrayList> internalids = collectionData.get(internalid);
        return internalids!=null; 
    }

    public Map<String, Map<String, ArrayList>> getCollectionDataInstance() {
        return collectionData;
    }
}
