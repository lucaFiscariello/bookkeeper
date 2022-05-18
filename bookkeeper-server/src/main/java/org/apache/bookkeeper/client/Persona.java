package org.apache.bookkeeper.client;

public class Persona {
    private String nome;

    public Persona(String nome){
        this.nome=nome;
    }

    public String getNome(int num){
        if(num==1)
            return nome;
        else
            return nome+"prova";
    }
}
