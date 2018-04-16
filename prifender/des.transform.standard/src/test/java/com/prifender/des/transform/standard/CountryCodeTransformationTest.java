package com.prifender.des.transform.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.TransformationException;

public final class CountryCodeTransformationTest
{
    @Test
    public void testUnitedStates() throws Exception
    {
        final Transformation t = new CountryCodeTransformation();
        
        assertEquals( "US", t.transform( "United States of America", null ) );
        assertEquals( "US", t.transform( "UNITED STATES OF aMeRiCa", null ) );
        assertEquals( "US", t.transform( "The United States of America", null ) );
        assertEquals( "US", t.transform( "USA", null ) );
        assertEquals( "US", t.transform( "US", null ) );
    }
    
    @Test
    public void testSouthKorea() throws Exception
    {
        final Transformation t = new CountryCodeTransformation();
        
        assertEquals( "KR", t.transform( "South Korea", null ) );
        assertEquals( "KR", t.transform( "SOUTH kOrEa", null ) );
        assertEquals( "KR", t.transform( "Republic of Korea", null ) );
        assertEquals( "KR", t.transform( "KOR", null ) );
        assertEquals( "KR", t.transform( "KR", null ) );
    }
    
    @Test
    public void testSandwichIslands() throws Exception
    {
        final Transformation t = new CountryCodeTransformation();
        
        assertEquals( "GS", t.transform( "South Georgia and South Sandwich Islands", null ) );
        assertEquals( "GS", t.transform( "South Georgia and the South Sandwich Islands", null ) );
    }
    
    @Test
    public void testRandomInput() throws Exception
    {
        final Transformation t = new CountryCodeTransformation();
        
        assertNull( t.transform( "abcdefg", null ) );
    }

    @Test
    public void testNullInput() throws Exception
    {
        final Transformation t = new CountryCodeTransformation();
        
        assertNull( t.transform( null, null ) );
    }
    
    @Test
    public void testWrongInputType() throws Exception
    {
        final Transformation t = new CountryCodeTransformation();
        TransformationException e;
        
        e = assertThrows( TransformationException.class, () -> t.transform( new Integer( 1 ), null ) );
        assertEquals( "The CountryCode transformation requires a String input, but received java.lang.Integer", e.getMessage() );

        e = assertThrows( TransformationException.class, () -> t.transform( new HashMap<String,Object>(), null ) );
        assertEquals( "The CountryCode transformation requires a String input, but received java.util.HashMap", e.getMessage() );
    }

}
