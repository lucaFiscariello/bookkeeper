/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.net;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(value = Enclosed.class)
public class MyNetworkTopologyTest {

    @RunWith(Parameterized.class)
    public static class TestSingleWrite  {

        private NetworkTopologyImpl networkTopology;
        int expected;
        private int numberAddedNode;
        private static String validRackTamplate= "/rack-0";
        private static String noValidRack="~/rack-null-error";
        private static String validBookieIDRack= "bookieIdScopeRack";
        private static String noValidbookieIDRack= "~bookieIdScopeRack-error";
        private String rack;
        private String bookieId;

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    {0,validRackTamplate,validBookieIDRack},
                    {1,validRackTamplate,validBookieIDRack},
                    {4,validRackTamplate,validBookieIDRack},
                    {0,noValidRack,validBookieIDRack},
                    {1,noValidRack,validBookieIDRack},
                    {4,noValidRack,validBookieIDRack},
                    {0,noValidRack,noValidbookieIDRack},
                    {1,noValidRack,noValidbookieIDRack},
                    {4,noValidRack,noValidbookieIDRack},

            });
        }

        public TestSingleWrite(int numberAddedNode, String rack, String id) {
            networkTopology = new NetworkTopologyImpl();
            this.bookieId=id;
            this.rack=rack;
            this.numberAddedNode=numberAddedNode;
            this.expected=getOracle();
        }

        @Test
        public void testSingleWrite()  {

            int actualValue;

            try{

                for(int i=0; i<numberAddedNode ;i++){
                    BookieId bookieIdScopeRack = BookieId.parse(bookieId+i);
                    BookieNode bookieRack0ScopeNode = new BookieNode(bookieIdScopeRack, rack);
                    networkTopology.add(bookieRack0ScopeNode);
                }

                Set<Node> leavesScopeRack = networkTopology.getLeaves(rack);
                actualValue=leavesScopeRack.size();

            }catch (IllegalArgumentException e){
                actualValue=-1;
            }

            assertEquals(expected, actualValue);

        }

        private int getOracle() {
            if(this.numberAddedNode==0)
                return 0;
            if(this.rack.equals(validRackTamplate) && this.bookieId.equals(validBookieIDRack))
                return this.numberAddedNode;
            else
                return -1;
        }


    }

    @RunWith(Parameterized.class)
    public static class TestMultipleWrite  {

        private NetworkTopologyImpl networkTopology;
        int expected;
        private int numberAddedNode;
        private int numberRack;
        private static String validRackTamplate= "/rack-0";
        private static String noValidRack="~/rack-null-error";
        private static String validBookieID= "bookieIdScopeRack";
        private static String noValidbookieID= "~bookieIdScopeRack-error";
        private String rack;
        private String bookieId;
        private TypeId typeid;
        private TypeRack typeRack;

        private enum TypeRack {
            ALL_VALID,
            ALL_NO_VALID,
            PARTIAL
        }

        private enum TypeId {
            ALL_VALID,
            ALL_NO_VALID,
            PARTIAL
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    /*racknumber,node added,type rack,typeid*/
                    {2,0,TypeRack.ALL_VALID,TypeId.ALL_VALID},
                    {2,1,TypeRack.ALL_VALID,TypeId.ALL_VALID},
                    {2,4,TypeRack.ALL_VALID,TypeId.ALL_VALID},
                    {2,0,TypeRack.ALL_NO_VALID,TypeId.ALL_VALID},
                    {2,1,TypeRack.ALL_NO_VALID,TypeId.ALL_VALID},
                    {2,4,TypeRack.ALL_NO_VALID,TypeId.ALL_VALID},
                    {2,0,TypeRack.ALL_NO_VALID,TypeId.ALL_NO_VALID},
                    {2,1,TypeRack.ALL_NO_VALID,TypeId.ALL_NO_VALID},
                    {2,4,TypeRack.ALL_NO_VALID,TypeId.ALL_NO_VALID},
                    {2,0,TypeRack.ALL_VALID,TypeId.ALL_NO_VALID},
                    {2,1,TypeRack.ALL_VALID,TypeId.ALL_NO_VALID},
                    {2,4,TypeRack.ALL_VALID,TypeId.ALL_NO_VALID},
                    {2,4,TypeRack.PARTIAL,TypeId.PARTIAL},
                    {4,0,TypeRack.ALL_VALID,TypeId.ALL_VALID},
                    {4,1,TypeRack.ALL_VALID,TypeId.ALL_VALID},
                    {4,4,TypeRack.ALL_VALID,TypeId.ALL_VALID},
                    {4,0,TypeRack.ALL_NO_VALID,TypeId.ALL_VALID},
                    {4,1,TypeRack.ALL_NO_VALID,TypeId.ALL_VALID},
                    {4,4,TypeRack.ALL_NO_VALID,TypeId.ALL_VALID},
                    {4,0,TypeRack.ALL_NO_VALID,TypeId.ALL_NO_VALID},
                    {4,1,TypeRack.ALL_NO_VALID,TypeId.ALL_NO_VALID},
                    {4,4,TypeRack.ALL_NO_VALID,TypeId.ALL_NO_VALID},
                    {4,0,TypeRack.ALL_VALID,TypeId.ALL_NO_VALID},
                    {4,1,TypeRack.ALL_VALID,TypeId.ALL_NO_VALID},
                    {4,4,TypeRack.ALL_VALID,TypeId.ALL_NO_VALID},
                    {4,4,TypeRack.PARTIAL,TypeId.PARTIAL},
            });
        }

        public TestMultipleWrite(int numberRack,int numberNode, TypeRack typeRack, TypeId typeId) {
            this.networkTopology=new NetworkTopologyImpl();
            this.numberAddedNode=numberNode;
            this.numberRack=numberRack;
            this.typeid=typeId;
            this.typeRack=typeRack;
            this.expected= getOracle();

            setup();
        }

        private int getOracle() {
            int allNode= numberAddedNode*numberRack;

            if(allNode==0)
                return allNode;
            if(typeid.equals(TypeId.ALL_VALID) && typeRack.equals(TypeRack.ALL_VALID))
                return numberAddedNode*numberRack;
            return -1;
        }

        public void setup(){
            if(typeid.equals(TypeId.ALL_VALID))
                bookieId=validBookieID;
            if(typeid.equals(TypeId.ALL_NO_VALID))
                bookieId=noValidbookieID;
            if(typeRack.equals(TypeRack.ALL_VALID))
                rack=validRackTamplate;
            if(typeRack.equals(TypeRack.ALL_NO_VALID))
                rack=noValidRack;
        }

        @Test
        public void testMultipleWrite()  {
            int actualValue=0;

            try{

                for(int i=0 ; i<numberRack; i++){
                    for(int j=0;j<numberAddedNode;j++){

                        if(typeid.equals(TypeId.PARTIAL)){

                            if(bookieId==null)
                                bookieId=noValidbookieID;
                            else
                                bookieId=(bookieId.equals(validBookieID))?noValidbookieID:validBookieID;

                        }

                        if(typeRack.equals(TypeRack.PARTIAL)){

                            if(rack==null)
                                rack=noValidRack;
                            else
                                rack=(rack.equals(validRackTamplate))?noValidRack:validRackTamplate;
                        }

                        BookieId bookieIdScopeRack = BookieId.parse(bookieId+j);
                        BookieNode bookieRackScopeNode = new BookieNode(bookieIdScopeRack, rack+i);
                        networkTopology.add(bookieRackScopeNode);
                    }
                }

                for(int k = 0;k<numberRack;k++){
                    Set<Node> leavesScopeRack = networkTopology.getLeaves(rack+k);
                    actualValue=actualValue+leavesScopeRack.size();
                }

            }catch (IllegalArgumentException e){
                actualValue=-1;
            }

            assertEquals(expected, actualValue);

        }

    }


    @RunWith(Parameterized.class)
    public static class TestGet {

        private NetworkTopologyImpl networkTopology;
        private Set<Node> expected;
        private Set<Node> allNodeGenerate;
        private int numberAddedNode;
        private int numberRack;
        private String rackexclude;
        private String bookieId;
        private static String rackTemplate= "/rack-";
        private static String validBookieID= "bookieIdScopeRack";
        private static String validRack= "~"+rackTemplate+"0";
        private static String validRackNoPresent="~"+rackTemplate+"--1";
        private static String noValidRack=null;


        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    /*rack number,nodeadded,rack,idbookie*/
                    {0,2,validRack,validBookieID},
                    {1,2,validRack,validBookieID},
                    {3,2,validRack,validBookieID},
                    {0,2,validRackNoPresent,validBookieID},
                    {1,2,validRackNoPresent,validBookieID},
                    {3,2,validRackNoPresent,validBookieID},
                    {0,2,noValidRack,validBookieID},
                    {1,2,noValidRack,validBookieID},
                    {3,2,noValidRack,validBookieID}
            });
        }

        public TestGet(int numberAddedNode, int numberRack, String rack,String bookieId) {
            networkTopology = new NetworkTopologyImpl();
            this.rackexclude=rack;
            this.numberAddedNode=numberAddedNode;
            this.numberRack=numberRack;
            this.bookieId=bookieId;

            setup();
            this.expected=getOracle();
        }

        @Test
        public void testget()  {
            Set<Node> actualValue ;

            try{
                actualValue = networkTopology.getLeaves(rackexclude);
            }catch (NullPointerException e){
                actualValue=null;
            }

            assertEquals(this.expected, actualValue);

        }

        private Set<Node> getOracle() {
            HashSet<Node> expected= new HashSet<>() ;

            if(rackexclude==null)
                return null;

            if(this.numberRack*numberAddedNode==0)
                return expected;

            if(rackexclude.equals(validRack)){

                for (Node node:allNodeGenerate) {
                    if(!rackexclude.contains(node.getNetworkLocation())){
                        expected.add(node);
                    }
                }
            }

            if(rackexclude.equals(validRackNoPresent))
                return allNodeGenerate;

            return expected;
        }

        public void setup(){

            allNodeGenerate = new HashSet<>();

            for(int i=0 ; i<numberRack; i++){
                for(int j=0;j<numberAddedNode;j++){
                    BookieId bookieIdScopeRack = BookieId.parse(bookieId+j);
                    BookieNode bookieRackScopeNode = new BookieNode(bookieIdScopeRack, rackTemplate+i);
                    allNodeGenerate.add(bookieRackScopeNode);
                    networkTopology.add(bookieRackScopeNode);
                }
            }
        }

    }

    @RunWith(MockitoJUnitRunner.class)
    public static class WhiteBoxTestRemove{

        private NetworkTopology networkTopology;
        private Set<Node> allNodeGenerate;
        private Set<Node> expected;
        private Node validNodeToRemove;
        private int numberRack=3;
        private int numberAddedNode=3;
        private String bookieId= "bookieIdScopeRack";
        private String rackTemplate= "/rack-";


        @Test
        public void test1(){
            Set<Node> actualNodes = new HashSet<>();
            networkTopology.remove(null);

            for(int i=0 ; i<numberRack; i++)
                actualNodes.addAll(networkTopology.getLeaves(rackTemplate+i));

            assertEquals(actualNodes, allNodeGenerate);

        }

        @Test(expected = IllegalArgumentException.class)
        public void test2(){

            NetworkTopologyImpl.InnerNode node = mock(NetworkTopologyImpl.InnerNode.class);
            networkTopology.remove(node);

        }

        @Test
        public void test3(){

            Set<Node> actualNodes = new HashSet<>();

            networkTopology.remove(validNodeToRemove);

            for(int i=0 ; i<numberRack; i++)
                actualNodes.addAll(networkTopology.getLeaves(rackTemplate+i));

            assertEquals(expected,actualNodes);

        }

        @Test(expected =IllegalArgumentException.class )
        public void test4(){

            BookieId bookieIdScopeRack = BookieId.parse(bookieId);
            BookieNode node = new BookieNode(bookieIdScopeRack, null);
            networkTopology.remove(node);

        }


        @Test
        public void test5(){
            Set<Node> actualNodes = new HashSet<>();

            Node node = mock(Node.class);
            when(node.getNetworkLocation()).thenReturn("fail Location");
            networkTopology.remove(node);

            for(int i=0 ; i<numberRack; i++)
                actualNodes.addAll(networkTopology.getLeaves(rackTemplate+i));

            assertEquals(allNodeGenerate,actualNodes);

        }


        @Before
        public void setup(){

            networkTopology= new NetworkTopologyImpl();
            allNodeGenerate = new HashSet<>();
            expected = new HashSet<>();

            BookieNode bookieRackScopeNode=null;

            for(int i=0 ; i<numberRack; i++){
                for(int j=0;j<numberAddedNode;j++){
                    BookieId bookieIdScopeRack = BookieId.parse(bookieId+j+i);
                    bookieRackScopeNode = new BookieNode(bookieIdScopeRack, rackTemplate+i);
                    networkTopology.add(bookieRackScopeNode);
                    allNodeGenerate.add(bookieRackScopeNode);
                    expected.add(bookieRackScopeNode);
                }
            }

            validNodeToRemove=bookieRackScopeNode;
            expected.remove(validNodeToRemove);
        }
    }




}