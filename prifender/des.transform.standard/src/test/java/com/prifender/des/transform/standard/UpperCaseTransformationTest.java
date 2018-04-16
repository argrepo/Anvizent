package com.prifender.des.transform.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.TransformationException;
import com.prifender.des.transform.standard.UpperCaseTransformation;

public final class UpperCaseTransformationTest
{
    @Test
    public void testBasicScenarios() throws Exception
    {
        final Transformation t = new UpperCaseTransformation();
        
        assertEquals( "ABC", t.transform( "abc", null ) );
        assertEquals( "ABC", t.transform( "aBc", null ) );
        assertEquals( "ABC", t.transform( "ABC", null ) );
    }
    
    @Test
    public void testNullInput() throws Exception
    {
        final Transformation t = new UpperCaseTransformation();
        
        assertNull( t.transform( null, null ) );
    }
    
    @Test
    public void testWrongInputType() throws Exception
    {
        final Transformation t = new UpperCaseTransformation();
        TransformationException e;
        
        e = assertThrows( TransformationException.class, () -> t.transform( new Integer( 1 ), null ) );
        assertEquals( "The UpperCase transformation requires a String input, but received java.lang.Integer", e.getMessage() );

        e = assertThrows( TransformationException.class, () -> t.transform( new HashMap<String,Object>(), null ) );
        assertEquals( "The UpperCase transformation requires a String input, but received java.util.HashMap", e.getMessage() );
    }

}
