/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
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
    /**
     * lee un archivo
     * @param path 
     */
    private static void leerArchivo(String path,int parrafos_aleer ) {

        File archivo = new File(path);
        Scanner s = null;

        try {

            s = new Scanner(archivo);
            int contador = 0;
            int auxLineas = 0;
            String parrafos = "";

            while (s.hasNextLine()) {
                String linea = s.nextLine();
                auxLineas++;
                contador += linea.split("\n").length;
                parrafos += linea;
                if (contador == parrafos_aleer && linea.trim().length()!=0) {
                    escribirArchivo(parrafos, auxLineas);
                    auxLineas = 0;
                    parrafos = "";
                    contador = 0;
                }
            }

            if (contador < parrafos_aleer) escribirArchivo(parrafos, auxLineas);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (s != null) s.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    
    private static List<String> escribirArchivo(String texto, int numeroLineas) {
        List<String> archivosPath = new ArrayList<String>();
        try {

            File temp = File.createTempFile("temp" + numeroArchivosGenerados++ + "_" + numeroLineas + "_", ".txt", Paths.get("temp").toFile());

            FileWriter archivo = new FileWriter(temp);
            archivo.write(texto);
            archivo.close();
            archivosPath.add(temp.getPath());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return archivosPath;
    }
}
