package com.prifender.des.mock.simplified;

import static com.prifender.des.mock.MockDataExtractionServiceProblems.unknownCollection;
import static com.prifender.des.mock.data.DataSet.RNG;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.prifender.des.DataExtractionServiceException;
import com.prifender.des.mock.data.BirthDates;
import com.prifender.des.mock.data.CityStateZip;
import com.prifender.des.mock.data.Commissions;
import com.prifender.des.mock.data.Countries;
import com.prifender.des.mock.data.Emails;
import com.prifender.des.mock.data.FirstNames;
import com.prifender.des.mock.data.HireDates;
import com.prifender.des.mock.data.LastNames;
import com.prifender.des.mock.data.Nationalities;
import com.prifender.des.mock.data.PhoneNumbers;
import com.prifender.des.mock.data.Salaries;
import com.prifender.des.mock.data.SocialSecurityNumbers;
import com.prifender.des.mock.data.Streets;
import com.prifender.des.model.Metadata;
import com.prifender.des.model.NamedType;
import com.prifender.des.model.Type;

public final class SimplifiedDataSource
{
    public static final String TABLE_EMPLOYEES = "Employees";
    
    private static final Metadata METADATA = new Metadata()
        .addObjectsItem
        (
            new NamedType()
            .name( TABLE_EMPLOYEES )
            .type
            (
                new Type()
                .kind( Type.KindEnum.LIST )
                .entryType
                (
                    new Type()
                    .kind( Type.KindEnum.OBJECT )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "first_name" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "last_name" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "ssn" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "phone_mobile" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "phone_home" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "phone_office" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "email" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "addr_street" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "addr_city" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "addr_region" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "addr_postal_code" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "addr_country" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "hire_date" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DATE ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "dob" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DATE ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "nationality" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "salary" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DECIMAL ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "commission_pct" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DECIMAL ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "manager_id" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.INTEGER ) )
                    )
                )
            )
        );
    
    private final int size;
    
    public SimplifiedDataSource( final int size )
    {
        this.size = size;
    }
    
    public static boolean isValidTable( final String tableName )
    {
        for( final NamedType table : METADATA.getObjects() )
        {
            if( table.getName().equals( tableName ) )
            {
                return true;
            }
        }
        
        return false;
    }
    
    public static boolean isValidColumn( final String tableName, final String columnName )
    {
        for( final NamedType table : METADATA.getObjects() )
        {
            if( table.getName().equals( tableName ) )
            {
                final Type entryType = table.getType().getEntryType();
                
                for( final NamedType column : entryType.getAttributes() )
                {
                    if( column.getName().equals( columnName ) )
                    {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    public static Metadata metadata()
    {
        return METADATA;
    }
    
    public static Set<String> tables()
    {
        final Set<String> tables = new LinkedHashSet<String>();
        
        for( final NamedType table : METADATA.getObjects() )
        {
            tables.add( table.getName() );
        }
        
        return Collections.unmodifiableSet( tables );
    }
    
    public Iterator<Map<String,Object>> table( final String name ) throws DataExtractionServiceException
    {
        if( name != null )
        {
            if( name.equals( TABLE_EMPLOYEES ) )
            {
                return employees();
            }
        }
        
        throw new DataExtractionServiceException( unknownCollection( name ) );
    }
    
    public Iterator<Map<String,Object>> employees()
    {
        return new Iterator<Map<String,Object>>()
        {
            private int position = 0;
            private final Set<String> ssns = new HashSet<String>();
            
            @Override
            public boolean hasNext()
            {
                return ( this.position < SimplifiedDataSource.this.size );
            }

            @Override
            public Map<String,Object> next()
            {
                this.position++;
                
                final Map<String,Object> record = new LinkedHashMap<String,Object>();
                final String firstName = FirstNames.random();
                final String lastName = LastNames.random();
                final CityStateZip.Entry cityStateZip = CityStateZip.random();
                
                String ssn = null;
                
                while( ssn == null || this.ssns.contains( ssn ) )
                {
                    ssn = SocialSecurityNumbers.random();
                }
                
                this.ssns.add( ssn );
                
                record.put( "id", this.position );
                record.put( "first_name", firstName );
                record.put( "last_name", lastName );
                record.put( "ssn", ssn );
                record.put( "phone_mobile", PhoneNumbers.random() );
                record.put( "phone_home", PhoneNumbers.random() );
                record.put( "phone_office", PhoneNumbers.random() );
                record.put( "email", Emails.random( firstName, lastName ) );
                record.put( "addr_street", Streets.random() );
                record.put( "addr_city", cityStateZip.city );
                record.put( "addr_region", cityStateZip.state );
                record.put( "addr_postal_code", cityStateZip.zip );
                record.put( "addr_country", Countries.random() );
                record.put( "hire_date", HireDates.random() );
                record.put( "dob", BirthDates.random() );
                record.put( "nationality", Nationalities.random() );
                record.put( "salary", Salaries.random() );
                record.put( "commission_pct", Commissions.random() );
                
                int managerId = RNG.nextInt( SimplifiedDataSource.this.size ) + 1;
                
                while( managerId == this.position )
                {
                    managerId = RNG.nextInt( SimplifiedDataSource.this.size ) + 1;
                }
                
                record.put( "manager_id", managerId );
                
                return record;
            }
        };
    }
    
    public static void main( final String[] args ) throws Exception
    {
        final SimplifiedDataSource data = new SimplifiedDataSource( 5 );
        
        for( final String table : tables() )
        {
            System.out.println();
            System.out.println( table );
            System.out.println();
            
            for( final Iterator<Map<String,Object>> itr = data.table( table ); itr.hasNext(); )
            {
                System.out.println( mapToJson( itr.next() ) );
            }
        }
        
        System.out.println();
    }
    
    private static JsonObject mapToJson( final Map<String,Object> map )
    {
        final JsonObjectBuilder builder = Json.createObjectBuilder();
        
        for( final Map.Entry<String,Object> entry : map.entrySet() )
        {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            
            if( value instanceof String )
            {
                builder.add( key, (String) value );
            }
            else if( value instanceof Integer )
            {
                builder.add( key, (Integer) value );
            }
            else
            {
                throw new IllegalArgumentException();
            }
        }
        
        return builder.build();
    }

}
