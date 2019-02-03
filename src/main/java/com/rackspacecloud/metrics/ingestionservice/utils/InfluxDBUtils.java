package com.rackspacecloud.metrics.ingestionservice.utils;

public class InfluxDBUtils {

    public static String replaceSpecialCharacters(String inputString){
        final String[] metaCharacters = {"\\",":","^","$","{","}","[","]","(",")",".","*","+","?","|","<",">","-","&","%"," "};

        for (int i = 0 ; i < metaCharacters.length ; i++){
            if(inputString.contains(metaCharacters[i])){
                inputString = inputString.replace(metaCharacters[i],"_");
            }
        }
        return inputString;
    }
}
