package com.hbase.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class readProperties {
	
	public static String readConfPath(String key) {

    	Properties prop = new Properties();
    	InputStream input = null;
    	final String filename = "config.properties";
    	try {

    		input = readProperties.class.getClassLoader().getResourceAsStream(filename);
    		if(input==null){
    	        System.out.println("Unable to find " + filename);
    		    return "";
    		}

    		prop.load(input);

    	} catch (IOException ex) {
    		ex.printStackTrace();
    	} finally{
    		if(input!=null){
    			try {
    				input.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}
		return prop.getProperty(key);

    }
}
