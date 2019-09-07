/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduce;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author wcade
 */
public class Util {
    /**
     * crea solo una impresion de linea
     * @param linea 
     */
    public static void print(String linea){
        System.out.print(linea);
    }
    /**
     * Crea lineas
     * @param linea 
     */
    public static void println(String linea){
        System.out.print(linea);
    }
    /**
     * Imprime en pantalla
     * @param listaPalabras 
     */
    public static void imprimirPantalla( List<String> listaPalabras){
        String [] aux;
        for(int i=0; i < listaPalabras.size(); i++){
            aux = listaPalabras.get(i).split("\t");
            System.out.printf("[ %-15s %3d ] \t\t",aux[0], Integer.parseInt(aux[1]));
            if(i%4 == 3 ){System.out.print("\n");}
        }
        System.out.print("\n");
    }
    /**
     * Limpia Formatos
     * @param palabra_limpiar
     * @return 
     */
    private String limpiarFormato(String palabra_limpiar){
        Pattern p = Pattern.compile("[^a-zA-Z0-9']");
        Matcher m = p.matcher(palabra_limpiar);
        return m.replaceAll("").toLowerCase();
    }
}
