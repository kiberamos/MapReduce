/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduce;

import static java.lang.Thread.sleep;

/**
 *
 * @author wcade
 */
public class MapReduce {

    private static String numeroArchivosGenerados;

    /**
     * @param args the command line arguments
     */
    
    public static void main(String[] args) throws InterruptedException {

        
        Util.println("Se han creado  Mappers");
        crearHilosReducers();
        Util.println("Se han creado  Reducers\n******************************");
        crearHilosMap();
        

    }

    private static void crearHilosReducers() {
        
        Reduce r = new Reduce();
    
    }
     private static void crearHilosMap() {

        Map m = new Map();

    }
    
}
