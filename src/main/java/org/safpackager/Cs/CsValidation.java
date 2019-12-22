package org.safpackager.Cs;

import com.csvreader.CsvReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.safpackager.Utils;
import org.apache.commons.lang.StringUtils;

public class CsValidation {

    private CsvReader zipcodeReader;
    private CsvReader stateReader;
    private Map<String, Map<String, ArrayList>> zipcodeData = new HashMap<String, Map<String, ArrayList>>();
    private Map<String, String> statesData = new HashMap<String, String>();
    private static CsValidation validation = null;
    private File[] listOfImages = null;

    public CsValidation() {
        try {
            getStateData();
            getZipcodeData();
        } catch (IOException ex) {
            Logger.getLogger(CsValidation.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Map<String, ArrayList> validateCSV(String csvPath) throws IOException {
        Map<String, ArrayList> errors = new HashMap<String, ArrayList>();
        ArrayList<String> invalidHeadings = new ArrayList<String>();
        ArrayList<String> invalidZipcode = new ArrayList<String>();
        ArrayList<String> zipAddressNotMached = new ArrayList<String>();

        CsvReader inputCSV = Utils.openCsv(csvPath);
        inputCSV.readHeaders();

        for (int j = 0; j < inputCSV.getHeaderCount(); j++) {
            String s = inputCSV.getHeader(j);
            String s_l = s.toLowerCase();
            String v = inputCSV.get(j);
            if(!(s_l.contains("Internal ID".toLowerCase()) || s_l.contains("Sample ID".toLowerCase()) || s_l.contains("Date Collected".toLowerCase()) || s_l.contains("LIB".toLowerCase())
                     || s_l.contains("City".toLowerCase()) || s_l.contains("State".toLowerCase()) || s_l.contains("Zip".toLowerCase()) || s_l.contains("Screen Status".toLowerCase()) || s_l.contains("Photo".toLowerCase())
                     || s_l.contains("# of isolates from RBM".toLowerCase()) || s_l.contains("# of isolates from TV8".toLowerCase()) || s_l.contains("Collection Detail".toLowerCase()))){
                invalidHeadings.add(s);
            }
        }
        while(inputCSV.readRecord()) {
            int col = 0;
            String zip = null;
            String city = null;
            String state = null;
            String[] values = inputCSV.getValues();
            for(String s : inputCSV.getHeaders()) {
                s = s.toLowerCase();
                if(s.contains("Zip".toLowerCase())) {
                    zip = values[col];
                    try {
                        int zipCode = Integer.parseInt(values[col]);
                        if(zipCode > 99999 || zipCode < 1){
                            invalidZipcode.add(inputCSV.get("Sample ID"));
                        }
                    } catch(Exception e) {
                        System.out.println(e.getMessage());;
                    }
                }
                if(s.contains("City".toLowerCase())) {
                    city = values[col];
                }
                if(s.contains("State".toLowerCase())) {
                    state = values[col];
                }
                col++;
            }
            if(!isZipPlaceMatched(zip, state, city)){
                zipAddressNotMached.add(inputCSV.get("Sample ID"));
            }
        }
        if(!invalidHeadings.isEmpty()) {
            errors.put("invalidHeadings", invalidHeadings);
        }
        if(!invalidZipcode.isEmpty()) {
            errors.put("invalidZipcode", invalidZipcode);
        }
        if(!zipAddressNotMached.isEmpty()) {
            errors.put("zipAddressNotMached", zipAddressNotMached);
        }
        return errors;
    }

    private boolean isZipPlaceMatched(String homeZip, String homeState, String homeCity){
        if(homeZip == null || "".equals(homeZip) || homeState == null || "".equals(homeState)|| homeCity == null || "".equals(homeCity)) {
            return false;
        }
        Map<String, ArrayList> zipdata = zipcodeData.get(homeZip);
        List<String> stateData = null;
        List<String> cityData = null;

        if(zipdata != null) {
            stateData = zipdata.get("state");
            cityData = zipdata.get("city");

            if(StringUtils.equals(stateData.get(0), homeState) && !homeCity.isEmpty()){
                return true;
            }
        }
        return false;
    }

    private void getZipcodeData() throws IOException {
        if(zipcodeData.size() == 0) {
            if(zipcodeReader == null) {
                String zipDataFile = getClass().getClassLoader().getResource("zipcode.csv").getFile();
                zipcodeReader = Utils.openCsv(zipDataFile);
                zipcodeReader.readHeaders();
            }
        }
        int rowCount = 0;
        while (zipcodeReader.readRecord()) {
            Map<String, ArrayList> items = new TreeMap<String, ArrayList>();
            int columnCount = 0;
            String samplId = null;
            String[] values = zipcodeReader.getValues();
            for(String s : zipcodeReader.getHeaders()) {
                s = s.toLowerCase();
                if(s.contains("zip".toLowerCase())) {
                    samplId = values[columnCount];
                }
                add(items, s, values[columnCount++]);
            }

            zipcodeData.put(samplId, items);
            rowCount++;
        }
    }

    private void getStateData() throws IOException {
        if(statesData.size() == 0) {
            if(stateReader == null) {
                String stateDataFile = getClass().getClassLoader().getResource("states.csv").getFile();
                stateReader = Utils.openCsv(stateDataFile);
                stateReader.readHeaders();
            }
        }

        while (stateReader.readRecord()) {
            if(stateReader.getValues().length == 2) {
                statesData.put(stateReader.getValues()[0], stateReader.getValues()[1]);
            }
        }
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


    public boolean isPhotoValidated(Map<String, ArrayList> errors) {
        List<String> invalidNames = errors.get("invalidNames");
        List<String> invalidPairs = errors.get("invalidPairs");
        
        return invalidNames.isEmpty() && invalidPairs.isEmpty();
    }
    
    public Map<String, ArrayList> validatePhotoNameFormat(String sourceDir) {

        Map<String, ArrayList> errors = new HashMap<String, ArrayList>();
        errors.put("invalidNames", new ArrayList<String>());
        errors.put("invalidPairs", new ArrayList<String>());
        Map<String, Integer> items = new HashMap<String, Integer>();

        File input = new File(sourceDir);
        FilenameFilter filter;
        filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                String lowercaseName = name.toLowerCase();
                return lowercaseName.endsWith(".jpg");
            }
        };
        listOfImages = input.listFiles(filter);

        for(File f : listOfImages)
        {
            String imgName = f.getName();
            boolean isBadFormat = Utils.isNamePatternMatched(imgName);
            if(!isBadFormat){
                errors.get("invalidNames").add(imgName);
            }

            String key = Utils.getImageKey(imgName);
            if(key != null && !"".equals(key)){
                if(items.get(key) == null){
                    items.put(key, 1);
                }else{
                    int count = items.get(key);
                    items.put(key, ++count);
                }
            }
        }
        
        Set<String> keys = items.keySet();
        for(String key : keys)
        {
            if(items.get(key) != 2){
                errors.get("invalidPairs").add(key);
            }
        }
        return errors;
    }

    public Map<String, String> getStatesData() {
        return statesData;
    }

    public Map<String, Map<String, ArrayList>> getZipData() {
        return zipcodeData;
    }

    public File[] getPhotoList() {
        return listOfImages;
    }

    public static CsValidation getInstance(){
        if(validation == null){
            validation = new CsValidation();
        }
        return validation;
    }

}
