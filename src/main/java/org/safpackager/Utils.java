package org.safpackager;

import com.csvreader.CsvReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import static org.safpackager.SAFPackage.detectCharsetOfFile;

public class Utils {

    public static enum IdType {
        SAMPLEID,
        INTERNALID
    }

    public static CsvReader openCsv(String absolutePath) throws IOException {
        CsvReader inputCSV = null;
        InputStream csvStream = new FileInputStream(absolutePath);
        inputCSV = new CsvReader(csvStream, detectCharsetOfFile(absolutePath));

        return inputCSV;

    }

    public static String getCsErrors(Map<String, ArrayList> errors) {
        List<String> invalidHeadings = errors.get("invalidHeadings");
        List<String> invalidZipcode = errors.get("invalidZipcode");
        List<String> zipAddressNotMached = errors.get("zipAddressNotMached");
        
        String e1 = "Invalid Headings: ";
        String e2 = "Invalid Zip: ";
        String e3 = "Zip Address Not Mached: ";

        String output = "<html>";
        if(invalidHeadings != null && invalidHeadings.size() > 0) {
            output += e1 + invalidHeadings + "<br/>";
        }
        if(invalidZipcode != null && invalidZipcode.size() > 0) {
            output += e2 + invalidZipcode + "<br/>";
        }
        if(zipAddressNotMached != null && zipAddressNotMached.size() > 0) {
            output += e3 + zipAddressNotMached + "<br/>";
        }
        output += "</html>";
        return output;
    }

    public static String getPhotoErrors(Map<String, ArrayList> errors) {
        List<String> invalidNames = errors.get("invalidNames");
        List<String> invalidPairs = errors.get("invalidPairs");
        
        String e1 = "Invalid names: ";
        String e2 = "Need two photos: ";

        String output = "<html>";
        if(invalidNames.size() > 0) {
            output += e1 + invalidNames + "<br/>";
        }
        if(invalidPairs.size() > 0) {
            output += e2 + invalidPairs + "<br/>";
        }
        output += "</html>";
        return output;
    }

    public static String getCsErrorInfo(List<String> errors) {
        String output = "<html>";
        for(String e : errors) {
            output += e + "<br/>";
        }
        output += "</html>";
        return output;
    }
    
    public static boolean isNamePatternMatched(String name){
        boolean isMatched;
        String pattern = "^[^0]\\d+_[R|T]\\s2.jpg$";
        isMatched = name.matches(pattern);
        return isMatched;
    }

    public static String getImageKey(String imageTitle){
        String part = null;
        if(imageTitle != null && !"".equals(imageTitle)){
            part = imageTitle.split("_")[0];
        }
        return part;
    }

    public static String convertCsvMetadata(String field){
        String metadataString = null;
        field = field.trim();
        if((field != null) && !"".equals(field))
        {
            if(StringUtils.containsIgnoreCase(field, "Internal ID"))
                metadataString = "dwc.npdg.internalcode";
            else if(StringUtils.containsIgnoreCase(field, "Sample ID"))
                metadataString = "dwc.npdg.sampleid";
            else if(StringUtils.containsIgnoreCase(field, "Date Collected"))
                metadataString = "dwc.npdg.datecollected";
            else if(StringUtils.containsIgnoreCase(field, "City"))
                metadataString = "dwc.npdg.homecity";
            else if(StringUtils.containsIgnoreCase(field, "State"))
                metadataString = "dwc.npdg.homestate";
            else if(StringUtils.containsIgnoreCase(field, "Zip"))
                metadataString = "dwc.npdg.homezip";
            else if(StringUtils.containsIgnoreCase(field, "Screen Status"))
                metadataString = "screenstatus";
            else if(StringUtils.containsIgnoreCase(field, "# of isolates from RBM"))
                metadataString = "dwc.npdg.isolatesRBM";
            else if(StringUtils.containsIgnoreCase(field, "# of isolates from TV8"))
                metadataString = "dwc.npdg.isolatesTV8";
            else if(StringUtils.containsIgnoreCase(field, "Collection Detail"))
                metadataString = "dwc.npdg.detail";
            else if(StringUtils.containsIgnoreCase(field, "LIB"))
                metadataString = "lib";
            else if(StringUtils.containsIgnoreCase(field, "Photo"))
                metadataString = "dwc.npdg.imagestatus";
            else if(StringUtils.containsIgnoreCase(field, "Link"))
                metadataString = "dc.relation.wiki";
            else
                metadataString = field;
        }
        return metadataString;
    }

    public static String cleanMetadataField(String field) {
        return field.split("\\[")[0];
    }

    public static String getCtErrors(ArrayList<String> errors) {
        String e = "Invalid Internal Ids: ";

        String output = "<html>";
        if(errors.size() > 0) {
            output += e + errors + "<br/>";
        }
        output += "</html>";
        return output;
    }

    public static String getCollectionErrors(boolean validated) {
        String output = "";
        if(!validated) {
            String e = "Please use collection metadata file exported from ShareOK ";
            output = "<html>";
            output += e + "<br/>";
            output += "</html>";            
        }
        return output;
    }

    public static boolean validateCollectionCsv(String csvPath) throws IOException {
        String[] headers = {"id", "collection", "dc.date.accessioned", "dc.identifier.uri",
            "dc.relation.wiki","dwc.npdg.datecollected","dwc.npdg.detail","dwc.npdg.homecity",
            "dwc.npdg.homestate","dwc.npdg.homezip","dwc.npdg.imagestatus","dwc.npdg.internalcode",
            "dwc.npdg.isolatesRBM","dwc.npdg.isolatesTV8","dwc.npdg.sampleid","dwc.npdg.spatial"};

        CsvReader inputCSV = Utils.openCsv(csvPath);
        inputCSV.readHeaders();
        for (int j = 0; j < inputCSV.getHeaderCount(); j++) {
            String s = inputCSV.getHeader(j);
            String v = inputCSV.get(j);
            for(String h : headers) {
                if(s.contains(h)){
                    return true;
                }
            }
        }
        return false;
    }

    public static String getZipcodeErrors(boolean validated) {
        String output = "";
        if(!validated) {
            String e = "Please use zipcode csv file ";
            output = "<html>";
            output += e + "<br/>";
            output += "</html>";
        }
        return output;
    }

    public static boolean validateZipcodeCsv(String csvPath) throws IOException {
        String[] headers = {"zip", "city", "state", "latitude", "longitude"};

        CsvReader inputCSV = Utils.openCsv(csvPath);
        inputCSV.readHeaders();
        for (int j = 0; j < inputCSV.getHeaderCount(); j++) {
            String s = inputCSV.getHeader(j);
            for(String h : headers) {
                if(s.contains(h)){
                    return true;
                }
            }
        }
        return false;
    }
}
