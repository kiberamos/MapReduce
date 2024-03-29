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
    static int FALLOS_COMBINE = 2;
    public static int SEGUNDOS_DE_FALLO = 2;

    static Map[] maps;
    static Combine[] combines;
    
    
    static boolean END_MAPPERS = false;
    static boolean END_REDUCERS = false;
    /**
     * @param args the command line arguments
     */
    
    public static void main(String[] args) throws InterruptedException {
        leer("texto3.txt");
        Util.println("Se han creado  Reducers");
        System.out.println("--->"+archivosPath);        
        crearHilosMap();
        crearHilosReducers();

        int contador_mappers = 0, contador_reducers = 0;

        while (!END_MAPPERS || !END_REDUCERS) {

            for (int i = 0; i < maps.length; i++) {
                if (maps[i].ESTADO == 2 && !maps[i].LEIDO) {

                    System.out.println("\n\n[Mapper-0" + maps[i].getIdMap() + "]: TERMINÓ:");
                    System.out.println("RESULTADO: [Mapper-0" + maps[i].getIdMap() + "]: *** " + maps[i].getPathMap()[0]);
                    System.out.println("RESULTADO: [Mapper-0" + maps[i].getIdMap() + "]: *** " + maps[i].getPathMap()[1]+"\n\n");
                    maps[i].imprimirResultado();
                    maps[i].LEIDO = true;
                    contador_mappers++;
                    sleep(SEGUNDOS_DE_FALLO*1000);

                    for (int j = 0; j < combines.length / 2; j++) {
                        if (combines[j].getCantidadMappers() < 2 * NUMERO_MAPPERS_POR_REDUCER && combines[j] != null) {
                            combines[j].setPath(maps[i].getPathMap()[0]);

                            j = combines.length;
                        } else {
                            if (combines[j].getState() == Thread.State.NEW) {
                                System.out.println("\n\nSE HA ASIGNADO A Reducer-0" + combines[j].getIdReducer() + ":");
                                for (int k = 0; k < combines[j].getPath().size(); k++)
                                    System.out.println("[File](" + (k + 1) + "):" + combines[j].getPath().get(k));
                                sleep(SEGUNDOS_DE_FALLO*1000);
                                combines[j].start();
                            }
                        }
                    }
                    for (int j = combines.length / 2; j < combines.length; j++) {
                        if (combines[j].getCantidadMappers() < 2 * NUMERO_MAPPERS_POR_REDUCER && combines[j] != null) {
                            combines[j].setPath(maps[i].getPathMap()[1]);

                            j = combines.length;
                        } else {
                            if (combines[j].getState() == Thread.State.NEW) {
                                System.out.println("\n\nSE HA ASIGNADO A Reducer-0" + combines[j].getIdReducer() + ":");
                                for (int k = 0; k < combines[j].getPath().size(); k++)
                                    System.out.println("[File](" + (k + 1) + "):" + combines[j].getPath().get(k));
                                sleep(SEGUNDOS_DE_FALLO*1000);
                                combines[j].start();
                            }
                        }
                    }

                }
                sleep(SEGUNDOS_DE_FALLO*1000);
            }
            for (int i = 0; i < combines.length; i++) {
                if (combines[i].ESTADO == 2 && !combines[i].LEIDO) {
                    System.out.println("\n\n[Reducer-0" + combines[i].getIdReducer() + "]: TERMINÓ:");
                   combines[i].imprimirResultado();
                    // imprimirLista(reducer[i].getPathReduce(), "Reducer-0"+reducer[i].getIdReducer());
                    sleep(SEGUNDOS_DE_FALLO*1000);
//                    System.out.println("RESULTADO: [Reducer-0" + reducer[i].getIdReducer() + "]: *** " + reducer[i].getPathReduce());
                    combines[i].LEIDO = true;
                    contador_reducers++;
                }

            }
            if (END_MAPPERS) {
                for (int i = 0; i < combines.length; i++) {
                    if (combines[i].getState() == Thread.State.NEW) {
                        System.out.println("\nSE HA ASIGNADO A Reducer-0" + combines[i].getIdReducer() + ":");
                        for (int k = 0; k < combines[i].getPath().size(); k++)
                            System.out.println("[File](" + (k + 1) + "):" + combines[i].getPath().get(k));
                        sleep(SEGUNDOS_DE_FALLO * 1000);
                        combines[i].start();
                    }
                }
            }

            END_MAPPERS = contador_mappers == maps.length ? true : false;
            END_REDUCERS = contador_reducers == combines.length ? true : false;
        }
        combinarReduce();
    }

    private static void crearHilosReducers() {
        int aux = numeroArchivosGenerados % NUMERO_MAPPERS_POR_REDUCER != 0 ? 1 : 0;
        combines = new Combine[numeroArchivosGenerados / NUMERO_MAPPERS_POR_REDUCER + aux];
        for (int i = 0; i < combines.length; i++)
            combines[i] = new Combine(fallosCombine(), i + 1);
    
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
     
     private static boolean fallosCombine() {

        if (FALLOS_COMBINE > 0) {
            FALLOS_COMBINE--;
            return false;
        } else return true;

    }

     private static void iniciarHilos(Thread[] hilos) {
        for (int i = 0; i < hilos.length; i++)
            hilos[i].start();
    }
    private static void combinarReduce() {

        List<String> aux = new ArrayList<>();

        for (int i = 0; i < combines.length; i++)
            aux.addAll(leerFinal(combines[i].toString()));

        combinarFinal(aux);
    }
    private static List<String> leerFinal(String file) {
        List<String> a = new ArrayList<>();
        File archivo = new File(file);
        Scanner s = null;
        try {
            s = new Scanner(archivo);
            while (s.hasNextLine()) {
                a.add(s.nextLine());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            s.close();
            return a;
        }
    }

    private static void combinarFinal(List<String> listaPalabras) {
        Collections.sort(listaPalabras);
        List<String> aux = new ArrayList<String>();

        String palabraActual = null, palabra = "";
        int contador = -1;

        for (int i = 0; i < listaPalabras.size(); i++) {
            String[] a = listaPalabras.get(i).split("\t");

            palabra = a[0].trim();
            int frecuencia = Integer.parseInt(a[1].trim());

            if (palabra.equals(palabraActual)) {
                contador = contador + frecuencia;
            } else {
                if (palabraActual == null) {
                    palabraActual = palabra;
                    contador = frecuencia;

                } else {
                    aux.add(String.format("%s\t%d", palabraActual, contador));
                    palabraActual = palabra;
                    contador = frecuencia;
                }
            }

        }
        if (palabraActual == palabra) aux.add(String.format("%s\t%d", palabraActual, contador));
        escribirReduce(aux);

    }

    private static void escribirReduce(List<String> lista) {
        try {
            File tempFile = File.createTempFile("MAP-REDUCE", ".txt", Paths.get("reduce").toFile());
            FileWriter archivoWrite = new FileWriter(tempFile);
            for (int i = 0; i < lista.size(); i++)
                archivoWrite.write(lista.get(i) + "\n");

            archivoWrite.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            imprimirReduce(lista);
        }
    }
    public static void imprimirReduce(List<String> lista){
        String [] aux;
        Util.println("RESULTADO FINAL [MAPREDUCE]: ");
        for(int i=0; i < lista.size(); i++){
            aux = lista.get(i).split("\t");
            System.out.printf("< %-15s %3d > \t",aux[0], Integer.parseInt(aux[1]));
            if(i%5 == 4 ){System.out.print("\n");}
        }
    }
}
