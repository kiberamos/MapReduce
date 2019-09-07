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
import java.util.List;
import java.util.Scanner;

/**
 *
 * @author wcade
 */
public class MapReduce {

    /** util de lectura y escritudra de lista de archivos
     * 
     */
    static List<String> archivosPath = new ArrayList<String>();
        
    /**
     * contador
     */
    static int numeroArchivosGenerados = 0;
    final static int NUMERO_PARRAFOS = 25;

    /**
     * @param args the command line arguments
     */
    
    public static void main(String[] args) throws InterruptedException {

        Util.println("Se han creado  Reducers");
        crearHilosMap();
        

    }

    private static void crearHilosReducers() {
        
        Reduce r = new Reduce();
    
    }
     private static void crearHilosMap() {

        Map m = new Map();

    }
     private static void leer(String path) {

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
                if (contador == NUMERO_PARRAFOS) {
                    escribir(parrafos, auxLineas);
                    auxLineas = 0;
                    parrafos = "";
                    contador = 0;
                }
            }

            if (contador < NUMERO_PARRAFOS) escribir(parrafos, auxLineas);

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
     
     private static void escribir(String texto, int numeroLineas) {

        try {

            File temp = File.createTempFile("temp" + numeroArchivosGenerados++ + "_" + numeroLineas + "_", ".txt", Paths.get("temp").toFile());

            FileWriter archivo = new FileWriter(temp);
            archivo.write(texto);
            archivo.close();
            archivosPath.add(temp.getPath());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}
