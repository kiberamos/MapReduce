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
    final static int NUMERO_PARRAFOS = 50;
    final static int NUMERO_MAPPERS_POR_REDUCER = 3;
    static int FALLOS_MAP = 2;
    static int FALLOS_REDUCER = 2;
    public static int SEGUNDOS_DE_FALLO = 2;

    static Map[] maps;
    static Reduce[] reduces;
    /**
     * @param args the command line arguments
     */
    
    public static void main(String[] args) throws InterruptedException {
        leer("texto3.txt");
        Util.println("Se han creado  Reducers");
        System.out.println("--->"+archivosPath);        
        crearHilosMap();
        crearHilosReducers();

        
    }

    private static void crearHilosReducers() {
        int aux = numeroArchivosGenerados % NUMERO_MAPPERS_POR_REDUCER != 0 ? 1 : 0;
        reduces = new Reduce[numeroArchivosGenerados / NUMERO_MAPPERS_POR_REDUCER + aux];
        for (int i = 0; i < reduces.length; i++)
            reduces[i] = new Reduce(fallosReducer(), i + 1);
    
    }
     private static void crearHilosMap() {
        maps = new Map[numeroArchivosGenerados];

        for (int i = 0; i < numeroArchivosGenerados; i++)
            maps[i] = new Map(fallosMap(), archivosPath.get(i), i + 1);
        iniciarHilos(maps);
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
     
     private static boolean fallosMap() {

        if (FALLOS_MAP > 0) {
            FALLOS_MAP--;
            return false;
        } else return true;

    }
     
     private static boolean fallosReducer() {

        if (FALLOS_REDUCER > 0) {
            FALLOS_REDUCER--;
            return false;
        } else return true;

    }

     private static void iniciarHilos(Thread[] hilos) {
        for (int i = 0; i < hilos.length; i++)
            hilos[i].start();
    }
    
}
