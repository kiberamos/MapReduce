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
import static java.lang.Thread.sleep;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author wcade
 */
public class Map extends Thread {

        public int FLAG = 0; // 0 - terminará bien, 1 - tendrá un fallo,
    public int ESTADO = 0; // 0 - no ha sido asignado tarea, 1 - en ejecución, 2 - terminó

    public  boolean LEIDO = false;


    private String path;
    private String [] pathMap = new String[2];
    private List<String> listaPalabras = new ArrayList<String>();
    private int idMap = -888;

    public Map(boolean runOk, String path, int id) {

        FLAG = runOk ? 0 : 1;
        this.path = path;
        idMap = id;

    }
    @Override
    public void run(){
        ESTADO = 1;
        Util.println("\n[Map -"+idMap+"] HA INICIADO...");
        funcionMapper(path);
    }

    private void funcionMapper(String file){
        if(FLAG == 0){
            File archivo = new File(file);
            Scanner s = null;
            try{
                s = new Scanner(archivo);
                while (s.hasNextLine()) {
                    String[] palabras = s.nextLine().split(" ");
                    for(int i =0; i < palabras.length; i++){                   
                        String limpio = Util.limpiarFormato(palabras[i]);
                        if(limpio.trim().length()>0){
                            listaPalabras.add(limpio);
                        }                        
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }finally {
                if(s != null) s.close();
                Collections.sort(listaPalabras);
                escribir();
                ESTADO = 2;
            }
        }else {

            Util.err("(ERROR-M): [Mapper-0" + idMap + "]: Ha presentado una falla, se reiniciará en "+MapReduce.SEGUNDOS_DE_FALLO*2+" segundos\n");
            try {
                sleep(MapReduce.SEGUNDOS_DE_FALLO*2*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                FLAG = 0;
                Util.println("[Mapper-0" + idMap + "]: SE HA REINICIADO CON ÉXITO");
                funcionMapper(file);
            }

        }

    }

    private void escribir(){

        try {
            File tempFile = File.createTempFile("Mapper0"+idMap+"_A-M_" + idMap,".txt", Paths.get("temp").toFile());
            File tempFile2 = File.createTempFile("Mapper0"+idMap+"_N-Z_" + idMap,".txt", Paths.get("temp").toFile());

            FileWriter archivoWriter = new FileWriter(tempFile);
            FileWriter archivoWriter2 = new FileWriter(tempFile2);

            for (int i =0; i < listaPalabras.size(); i++){
                if ((listaPalabras.get(i).compareToIgnoreCase("m") <= 0)) {
                    archivoWriter.write(listaPalabras.get(i)+"\t"+1+",");
                } else {
                    archivoWriter2.write(listaPalabras.get(i)+"\t"+1+",");
                }
            }
            archivoWriter.close();
            archivoWriter2.close();
            pathMap[0] = tempFile.getPath();
            pathMap[1] = tempFile2.getPath();
        } catch (IOException e) {
            e.printStackTrace();

        }finally {

        }
    }

    public List<String> getListaPalabras() {
        return listaPalabras;
    }

    public int getIdMap() {
        return idMap;
    }

    public String[] getPathMap() {
        return pathMap;
    }

    public void imprimirResultado(){
        int contador = 0;
        for(int i=0; i < listaPalabras.size(); i++){
            System.out.printf("< %-15s %1d > \t\t",listaPalabras.get(i),1);

            if(i%4 == 3 ){ System.out.print("\n");}
        }
        System.out.print("\n");

    }
    
}
