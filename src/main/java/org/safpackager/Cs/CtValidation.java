package org.safpackager.Cs;

import com.csvreader.CsvReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.safpackager.SAFPackage.detectCharsetOfFile;
import org.safpackager.Utils;
import org.safpackager.Utils.IdType;

public class CtValidation {

    private Map<String, Map<String, ArrayList>> collectionData = new HashMap<String, Map<String, ArrayList>>();
    private String ctCollectionPath = null;
    private static CtValidation validation = null;

    public CtValidation(String pathToCollection, IdType idtype) {
        ctCollectionPath = pathToCollection;
        try {
            if(idtype == IdType.SAMPLEID) {
                getCollectionDataBySampleid();
            } else if(idtype == IdType.INTERNALID) {
                getCollectionDataByInternalid();
            }
        } catch (IOException ex) {
            Logger.getLogger(CsValidation.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    public static boolean validateTaxCsvFormat(String taxCsv, IdType idType) throws IOException {
        String[] headers_internal = {"Internal ID", "Taxonomy", "Link"};
        String[] headers_sample = {"Sample ID", "Taxonomy", "Link"};

        InputStream csvStream = new FileInputStream(taxCsv);
        CsvReader inputCSV = new CsvReader(csvStream, detectCharsetOfFile(taxCsv));

        if(csvStream.read() == 239 & csvStream.read() == 187 & csvStream.read() == 191){
            System.out.println("UTF-8 with BOM, bytes discarded");
        }

        inputCSV.readHeaders();
        String[] csvHeaders = inputCSV.getHeaders();
        List<String> listHeaders = Arrays.asList(csvHeaders);

        for(int i=0;i<listHeaders.size();i++) {
            listHeaders.set(i, listHeaders.get(i).toLowerCase());
        }

        if(idType == IdType.SAMPLEID) {
            for(String h : headers_sample) {
                if(!listHeaders.contains(h.toLowerCase())) {
                    return false;
                }
            }
        } else if(idType == IdType.INTERNALID) {
            for(String h : headers_internal) {
                if(!listHeaders.contains(h.toLowerCase())) {
                    return false;
                }
            }
        }
        return true;
    }

    public static String getTaxCsvFormatErrors(boolean validated) {
        String output = "";
        if(!validated) {
            String e = "Please use taxonomy csv file ";
            output = "<html>";
            output += e + "<br/>";
            output += "</html>";
        }
        return output;
    }

    public ArrayList<String> validateTaxCsvContent(String csvPath) throws IOException {
        ArrayList<String> errors = new ArrayList<String>();
        CsvReader inputCsv = Utils.openCsv(csvPath);
        inputCsv.readHeaders();

        while(inputCsv.readRecord()) {
            int col = 0;
            String[] values = inputCsv.getValues();
            for(String s : inputCsv.getHeaders()) {
                s = s.toLowerCase();
                if(s.contains("Sample ID".toLowerCase())) {
                    String sampleId = values[col];
                    // String internalId = values[col];
                    if(!hasSampleID(sampleId)) {
                        errors.add(sampleId);
                    }
                    break;
                }
                col++;
            }
        }
        return errors;
    }


    private void getCollectionDataBySampleid() throws IOException {
        CsvReader collectionReader;
        collectionReader = Utils.openCsv(ctCollectionPath);
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
            if(sampleId != null && !"".equals(sampleId)) {
                collectionData.put(sampleId, items);
            }
        }
    }

    private void getCollectionDataByInternalid() throws IOException {
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

    private boolean hasSampleID(String sampleid) {
        Map<String, ArrayList> sampleids = collectionData.get(sampleid);
        return sampleids!=null;
    }


    public Map<String, Map<String, ArrayList>> getCollectionDataInstance() {
        return collectionData;
    }
}
