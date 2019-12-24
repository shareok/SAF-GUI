package org.safpackager.Cs;

import com.csvreader.CsvReader;
import com.google.gson.Gson;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.safpackager.Utils;

public class CgPackage {

    private Map<String, Map<String, ArrayList>> collectionGeoData = new HashMap<String, Map<String, ArrayList>>();
    private String cgCollectionPath = null;
    private String cgSavePath = null;
    private String itemSeparator1 = "";
    private String itemSeparator2 = "";

    public CgPackage(String pathToCollection, String pathToSave) {
        cgCollectionPath = pathToCollection;
        cgSavePath = pathToSave;
        try {
            getGeoData();
        } catch (IOException ex) {
            Logger.getLogger(CgPackage.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void getGeoData() throws IOException {
        if (collectionGeoData.isEmpty()) {
            CsvReader collectionReader;
            collectionReader = Utils.openCsv(cgCollectionPath);
            collectionReader.readHeaders();

            while (collectionReader.readRecord()) {
                Map<String, ArrayList> items = new TreeMap<String, ArrayList>();
                int columnCount = 0;
                String internalId = null;
                String sampleidId = null;
                String[] values = collectionReader.getValues();
                for (String s : collectionReader.getHeaders()) {
                    s = Utils.cleanMetadataField(s);
                    if (s.contains("dwc.npdg.internalcode".toLowerCase())) {
                        internalId = values[columnCount];
                        add(items, s, values[columnCount]);
                    }
                    if (s.contains("dwc.npdg.spatial".toLowerCase())) {
                        add(items, s, values[columnCount]);
                    }
                    if (s.contains("dc.identifier.uri".toLowerCase())) {
                        add(items, s, values[columnCount]);
                    }
                    if (s.contains("dwc.npdg.homecity".toLowerCase())) {
                        add(items, s, values[columnCount]);
                    }
                    if (s.contains("dwc.npdg.homestate".toLowerCase())) {
                        add(items, s, values[columnCount]);
                    }
                    if (s.contains("dwc.npdg.homezip".toLowerCase())) {
                        add(items, s, values[columnCount]);
                    }
                    if (s.contains("dwc.npdg.sampleid".toLowerCase())) {
                        sampleidId = values[columnCount];
                        add(items, s, values[columnCount]);
                    }
                    columnCount++;
                }
                if (internalId != null && !"".equals(internalId)) {
                    collectionGeoData.put(internalId, items);
                } else {
                    collectionGeoData.put(sampleidId, items);
                }
            }
        }
    }

    private Map<String, ArrayList> add(Map<String, ArrayList> items, String key, String value) {
        if (items.get(key) == null) {
            items.put(key, new ArrayList<String>());
        }
        if (value != null && !value.isEmpty()) {
            items.get(key).add(value);
        }
        return items;
    }

    private void processJsData() {

    }

    public void writeGeoJs() throws IOException {
        String jsFile = cgSavePath + "/points.js";
        File f = new File(jsFile);
        if(Files.exists(f.toPath())){
            FileUtils.deleteQuietly(f);
        }

        List<String> pointjs = new ArrayList<String>();
        BufferedWriter jsWriter = new BufferedWriter(new FileWriter(jsFile));
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();
        StringBuilder sb4 = new StringBuilder();
        StringBuilder sb5 = new StringBuilder();
        sb1.append("var titlelist = [");
        sb2.append("var spatiallist = [");
        sb3.append("var urllist = [");
        sb4.append("var placelist = [");
        sb5.append("var ziplist = [");
        String separator = "";

        for (String key : collectionGeoData.keySet()) {
            Map<String, ArrayList> values = collectionGeoData.get(key);
            sb1.append(itemSeparator1);
            sb2.append(itemSeparator1);
            sb3.append(itemSeparator1);
            sb4.append(itemSeparator1);
            sb5.append(itemSeparator1);
            itemSeparator1 = "\",";

            ArrayList<String> a1 = values.get("dwc.npdg.internalcode");
            ArrayList<String> a2 = values.get("dwc.npdg.spatial");
            ArrayList<String> a3 = values.get("dc.identifier.uri");
            ArrayList<String> a4 = values.get("dwc.npdg.homecity");
            ArrayList<String> a5 = values.get("dwc.npdg.homestate");
            ArrayList<String> a6 = values.get("dwc.npdg.homezip");
            ArrayList<String> a7 = values.get("dwc.npdg.sampleid");
            if ((a1.size() > 0 || a7.size() > 0) && a2.size() > 0 
                    && a3.size() > 0 && a4.size() > 0 && a5.size() > 0 && a6.size() > 0) {
                if (a1.size() > 0) {
                    sb1.append("\"").append(a1.get(0));
                } else {
                    sb1.append("\"").append(a7.get(0));
                }
                sb2.append("\"").append(a2.get(0));
                sb3.append("\"").append("/handle/").append(((String) a3.get(0)).split("hdl.handle.net\\/")[1]);
                sb4.append("\"").append(a4.get(0)).append(",").append(a5.get(0).split(" - ")[1]);
                sb5.append("\"").append(a6.get(0));
            }
        }
        sb1.append("\"]");
        sb2.append("\"]");
        sb3.append("\"]");
        sb4.append("\"]");
        sb5.append("\"]");

        pointjs.add(sb1.toString());
        pointjs.add(sb2.toString());
        pointjs.add(sb3.toString());
        pointjs.add(sb4.toString());
        pointjs.add(sb5.toString());
        WriteFile(pointjs, jsFile);
    }

    private static void WriteFile(List<String> list, String filename) {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(filename, "UTF-8");
            for (String element : list) {
                writer.println(element);
            }
            writer.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(Reader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Reader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public String toJson(Point point) {
        Gson gson = new Gson();
        return gson.toJson(point);
    }

    public void writeGeoJson(){
        String file = cgSavePath + "/points.json";
        File f = new File(file);
        if(Files.exists(f.toPath())){
            FileUtils.deleteQuietly(f);
        }

        try {
            FileOutputStream fos = new FileOutputStream(file);
            OutputStreamWriter osr = new OutputStreamWriter(fos, "UTF-8");
            PrintWriter out = new PrintWriter(osr);
            out.print("{\"points\": [");

            for (String key : collectionGeoData.keySet()) {
                Map<String, ArrayList> values = collectionGeoData.get(key);
                Point point = new Point(values);
                String pointString = toJson(point);
                pointString = itemSeparator2 + pointString;
                itemSeparator2 = ",";
                out.print(pointString);
            }
            out.print("]}");
            out.close();
        } catch (IOException e)
        {
            Logger.getLogger(CgPackage.class.getName()).log(Level.SEVERE, null, e);
            System.out.println(e);
            System.exit(1);
        }
    }
    
    public static final class Point {
        private String spatial;
        private String title;
        private String uri;
        private String place;
        private String zip;
        
        public Point(Map<String, ArrayList> values) {
            init(values);
        }

        private void init(Map<String, ArrayList> items) {
            ArrayList<String> a1 = items.get("dwc.npdg.internalcode");
            ArrayList<String> a2 = items.get("dwc.npdg.spatial");
            ArrayList<String> a3 = items.get("dc.identifier.uri");
            ArrayList<String> a4 = items.get("dwc.npdg.homecity");
            ArrayList<String> a5 = items.get("dwc.npdg.homestate");
            ArrayList<String> a6 = items.get("dwc.npdg.homezip");
            ArrayList<String> a7 = items.get("dwc.npdg.sampleid");
            if ((a1.size() > 0 || a7.size() > 0) && a2.size() > 0 
                    && a3.size() > 0 && a4.size() > 0 && a5.size() > 0 && a6.size() > 0) {
                if (a1.size() > 0) {
                    setTitle((String)a1.get(0));
                } else {
                    setTitle((String)a7.get(0));
                }
                setSpatial((String)a2.get(0));
                setUri((String)a3.get(0));
                setPlace((String)a4.get(0) + ", " + a5.get(0).split(" - ")[1]);
                setZip((String)a6.get(0));
            }
        }

        public String getSpatial() {
            return spatial;
        }

        public void setSpatial(String spatial) {
            this.spatial = spatial;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            if(uri != null || uri.equals("")){
                if(uri.contains("http:")){
                    String replace = uri.replace("http:", "https:");
                    uri = replace;
                }
            }
            this.uri = uri;
        }

        public String getPlace() {
            return place;
        }

        public void setPlace(String place) {
            this.place = place;
        }

        public String getZip() {
            return zip;
        }

        public void setZip(String zip) {
            this.zip = zip;
        }
    }
}
