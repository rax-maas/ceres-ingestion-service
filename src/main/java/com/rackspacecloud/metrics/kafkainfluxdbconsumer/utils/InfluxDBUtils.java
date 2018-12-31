package com.rackspacecloud.metrics.kafkainfluxdbconsumer.utils;

public class InfluxDBUtils {
    public static String escapeSpecialCharactersForInfluxdb(String inputString){
        final String[] metaCharacters = {"\\","^","$","{","}","[","]","(",")",".","*","+","?","|","<",">","-","&","%"," "};

        for (int i = 0 ; i < metaCharacters.length ; i++){
            if(inputString.contains(metaCharacters[i])){
                inputString = inputString.replace(metaCharacters[i],"\\"+metaCharacters[i]);
            }
        }
        return inputString;
    }

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
