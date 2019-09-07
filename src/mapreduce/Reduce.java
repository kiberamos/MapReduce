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
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 *
 * @author wcade
 */
public class Reduce extends Thread {

    public  int FLAG=0; // 0 - terminará bien, 1 - tendrá un fallo,
    public  int ESTADO = 0; // 0 - terminó o no ha sido asignado tarea, 1 - en ejecución

    public  boolean LEIDO = false;

    private List<String> path = new ArrayList<String>();;
    private String pathReduce;
    private List<String> listaPalabras = new ArrayList<String>();
    private int idReducer;
    

    public List<String> getListaPalabras() {
        return listaPalabras;
    }

    String reducerName;
    String token = ",";



    public Reduce(boolean runOk , List<String> path, int id) {

        FLAG = runOk ? 0 : 1;
        this.path = path;
        idReducer = id;
        reducerName = "Reducer_"+ idReducer +"_";
        //funcionReducer(path);
    }
    public Reduce(boolean runOk , int id) {

        FLAG = runOk ? 0 : 1;
        idReducer = id;
        reducerName = "Reducer_"+ idReducer +"_";
        //funcionReducer(path);
    }

    @Override
    public void run(){
        System.out.println("\n[Reduce -"+idReducer+"] HA INICIADO...\n");
        funcionReducer(path);

    }

    private void funcionReducer(List<String>  file) {
        ESTADO = 1;

        if(FLAG == 0){
            for(int i=0; i < file.size(); i++){
                leer(file.get(i));
            }
            combinar();
            ESTADO = 2;

        }
        if(ESTADO == 1 && FLAG == 1){
            System.out.println("\n(ERROR-R):[Reducer-0" + idReducer + "]: Ha presentado una falla, se reiniciará en "+MapReduce.SEGUNDOS_DE_FALLO*2+" segundos/n");
            try{
                Thread.sleep(MapReduce.SEGUNDOS_DE_FALLO*2*1000);
                FLAG = 0;
                System.out.println("\n[Reducer-0" + idReducer + "]: SE HA REINICIADO CON ÉXITO\n");
                funcionReducer(file);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private void leer(String file) {
        File archivo = new File(file);
        Scanner s = null;
        try{
            s = new Scanner(archivo);
            while (s.hasNextLine()){
                String [] palabras = s.nextLine().split(token);
                for(int i=0; i < palabras.length-1;i++ ){
                    listaPalabras.add(palabras[i]);
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally {
            s.close();
        }
    }

    private void combinar(){
        Collections.sort(listaPalabras);
        List<String> aux = new ArrayList<String>();

        String palabraActual = null, palabra= "";
        int contador = -1;

        for(int i=0;i < listaPalabras.size();i++){
            String [] a = listaPalabras.get(i).split("\t");

            palabra = a[0].trim();
            int frecuencia = Integer.parseInt(a[1].trim());

            if (palabra.equals(palabraActual)){
                contador = contador+frecuencia;
            }
            else {
                if(palabraActual == null){
                    palabraActual = palabra;
                    contador = frecuencia;

                }else {
                    aux.add(String.format("%s\t%d", palabraActual, contador));
                    palabraActual = palabra;
                    contador = frecuencia;
                }
            }

        }
        if (palabraActual == palabra) aux.add(String.format("%s\t%d",palabraActual,contador));
        escribir(aux);
        Util.println("COMBINACION TERMINADA");
    }
    private void escribir(List<String> lista){
        try{
            File tempFile = File.createTempFile(reducerName,".txt", Paths.get("temp").toFile());
            FileWriter archivoWrite = new FileWriter(tempFile);
            for(int i=0; i < lista.size(); i++)
                archivoWrite.write(lista.get(i)+"\n");

            archivoWrite.close();
            pathReduce = tempFile.getPath();

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            listaPalabras = lista;
        }
    }
    public int getCantidadMappers(){
        if(path.size() > 0)
            return path.size();

        return 0;
    }

    public List<String> getPath() {
        return path;
    }

    public String getPathReduce() {
        return pathReduce;
    }

    public int getIdReducer() {
        return idReducer;
    }

    @Override
    public String toString() {
        return getPathReduce();
    }

    public void setPath(String str) {
        this.path.add(str);
    }

    public void imprimirResultado(){
        String [] aux;
        for(int i=0; i < listaPalabras.size(); i++){
            aux = listaPalabras.get(i).split("\t");
            System.out.printf("[ %-15s %3d ] \t\t",aux[0], Integer.parseInt(aux[1]));
            if(i%4 == 3 ){System.out.print("\n");}
        }
        System.out.print("\n");
    }
    
}
