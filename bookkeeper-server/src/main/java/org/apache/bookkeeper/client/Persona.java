package org.apache.bookkeeper.client;

public class Persona {
    private final String  nome;

    public Persona(String nome){
        this.nome=nome;
    }

    public String getNome(int num,int num2){
        int num3 = num+num2;
        String nome1;
        String nome2;

        if(num3==1){
            nome1=nome+"1";
            return nome1;
        }
        else{
            nome2=nome+"2";
            return nome2;
        }

    }
}
