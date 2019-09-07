/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduce;

import java.util.List;

/**
 *
 * @author wcade
 */
public class Util {
    public static void print(String linea){
        System.out.println(linea);
    }
    public static void println(String linea){
        Util.print(linea);
    }
    /*
    * crea una impresion de lineas
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
}
