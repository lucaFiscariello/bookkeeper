package org.apache.bookkeeper.client.impl;

import org.apache.bookkeeper.client.Persona;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PersonaTest {
    @Test
    public void test(){
        Persona p= new Persona("nome");
        assertEquals("nome", p.getNome(1));
    }

    @Test
    public void test2(){
        Persona p= new Persona("nome");
        assertEquals("nomeprova", p.getNome(2));
    }
    
}
