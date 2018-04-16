package com.prifender.des.transform.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.prifender.des.transform.Transformation;
import com.prifender.des.transform.TransformationException;
import com.prifender.des.transform.standard.TrimTransformation;

public final class TrimTransformationTest
{
    @Test
    public void testBasicScenarios() throws Exception
    {
        final Transformation t = new TrimTransformation();
        
        assertEquals( "abc", t.transform( "   abc", null ) );
        assertEquals( "abc", t.transform( "abc   ", null ) );
        assertEquals( "abc", t.transform( "   abc   ", null ) );
        assertEquals( "abc", t.transform( "abc", null ) );
        assertEquals( "abc  def", t.transform( "  abc  def  ", null ) );
    }
    
    @Test
    public void testNullInput() throws Exception
    {
        final Transformation t = new TrimTransformation();
        
        assertNull( t.transform( null, null ) );
    }
    
    @Test
    public void testWrongInputType() throws Exception
    {
        final Transformation t = new TrimTransformation();
        TransformationException e;
        
        e = assertThrows( TransformationException.class, () -> t.transform( new Integer( 1 ), null ) );
        assertEquals( "The Trim transformation requires a String input, but received java.lang.Integer", e.getMessage() );

        e = assertThrows( TransformationException.class, () -> t.transform( new HashMap<String,Object>(), null ) );
        assertEquals( "The Trim transformation requires a String input, but received java.util.HashMap", e.getMessage() );
    }

}
