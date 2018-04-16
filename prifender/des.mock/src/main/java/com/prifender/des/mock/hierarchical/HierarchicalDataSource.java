package com.prifender.des.mock.hierarchical;

import static com.prifender.des.mock.data.DataSet.RNG;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

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

public final class HierarchicalDataSource
{
    public static final String COLLECTION_EMPLOYEES = "Employees";
    
    private static final Metadata METADATA = new Metadata()
        .addObjectsItem
        (
            new NamedType()
            .name( COLLECTION_EMPLOYEES )
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
                        .name( "email" )
                        .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "addresses" )
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
                                    .name( "street" )
                                    .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                                )
                                .addAttributesItem
                                (
                                    new NamedType()
                                    .name( "city" )
                                    .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                                )
                                .addAttributesItem
                                (
                                    new NamedType()
                                    .name( "region" )
                                    .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                                )
                                .addAttributesItem
                                (
                                    new NamedType()
                                    .name( "postal_code" )
                                    .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                                )
                                .addAttributesItem
                                (
                                    new NamedType()
                                    .name( "country" )
                                    .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                                )
                            )
                        )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "phone_numbers")
                        .type
                        (
                            new Type()
                            .kind( Type.KindEnum.LIST )
                            .entryType( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.STRING ) )
                        )
                    )
                    .addAttributesItem
                    (
                        new NamedType()
                        .name( "pay_checks")
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
                                    .name( "payment_date" )
                                    .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DATE ) )
                                )
                                .addAttributesItem
                                (
                                    new NamedType()
                                    .name( "ammount" )
                                    .type( new Type().kind( Type.KindEnum.VALUE ).dataType( Type.DataTypeEnum.DECIMAL ) )
                                )
                            )
                        )
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

    private final SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy.MM.dd" );
    private final int size;
    
    public HierarchicalDataSource( final int size )
    {
        this.size = size;
    }
    
    public static boolean isValidCollection( final String name )
    {
        return COLLECTION_EMPLOYEES.equals( name );
    }
    
    public static Metadata metadata()
    {
        return METADATA;
    }

    public Iterator<JsonObject> iterator()
    {
        return new Iterator<JsonObject>()
        {
            private int position = 0;
            private final Set<String> ssns = new HashSet<String>();
            
            @Override
            public boolean hasNext()
            {
                return ( this.position < HierarchicalDataSource.this.size );
            }

            @Override
            public JsonObject next()
            {
                this.position++;
                
                final JsonObjectBuilder builder = Json.createObjectBuilder();
                final String firstName = FirstNames.random();
                final String lastName = LastNames.random();
                
                String ssn = null;
                
                while( ssn == null || this.ssns.contains( ssn ) )
                {
                    ssn = SocialSecurityNumbers.random();
                }
                
                this.ssns.add( ssn );
                
                builder.add( "id", this.position );
                builder.add( "first_name", firstName );
                builder.add( "last_name", lastName );
                builder.add( "ssn", ssn );
                builder.add( "email", Emails.random( firstName, lastName ) );
                builder.add( "hire_date", HireDates.random() );
                builder.add( "dob", BirthDates.random() );
                builder.add( "nationality", Nationalities.random() );
                builder.add( "salary", Salaries.random() );
                builder.add( "commission_pct", Commissions.random() );
                
                int managerId = RNG.nextInt( HierarchicalDataSource.this.size ) + 1;
                
                while( managerId == this.position )
                {
                    managerId = RNG.nextInt( HierarchicalDataSource.this.size ) + 1;
                }
                
                builder.add( "manager_id", managerId );
                
                final int phoneNumbersCount = RNG.nextInt( 4 );
                
                if( phoneNumbersCount > 0 )
                {
                    final JsonArrayBuilder phoneNumbersArrayBuilder = Json.createArrayBuilder();
                    
                    for( int i = 0; i < phoneNumbersCount; i++ )
                    {
                        phoneNumbersArrayBuilder.add( PhoneNumbers.random() );
                    }
                    
                    builder.add( "phone_numbers", phoneNumbersArrayBuilder.build() );
                }
                
                final int addressesCount = RNG.nextInt( 3 );

                if( addressesCount > 0 )
                {
                    final JsonArrayBuilder addressesArrayBuilder = Json.createArrayBuilder();
                    
                    for( int i = 0; i < addressesCount; i++ )
                    {
                        final JsonObjectBuilder childObjectBuilder = Json.createObjectBuilder();
                        final CityStateZip.Entry cityStateZip = CityStateZip.random();
                        
                        childObjectBuilder.add( "street", Streets.random() );
                        childObjectBuilder.add( "city", cityStateZip.city );
                        childObjectBuilder.add( "region", cityStateZip.state );
                        childObjectBuilder.add( "postal_code", cityStateZip.zip );
                        childObjectBuilder.add( "country", Countries.random() );
                        
                        addressesArrayBuilder.add( childObjectBuilder.build() );
                    }
                    
                    builder.add( "addresses", addressesArrayBuilder.build() );
                }
                
                final int payChecksCount = RNG.nextInt( 21 );
                
                if( payChecksCount > 0 )
                {
                    final JsonArrayBuilder payChecksArrayBuilder = Json.createArrayBuilder();
                    
                    final Calendar cal = Calendar.getInstance();
                    cal.set( Calendar.DAY_OF_MONTH, 5 );
                    
                    for( int i = 0; i < payChecksCount; i++ )
                    {
                        final JsonObjectBuilder childObjectBuilder = Json.createObjectBuilder();
                        
                        cal.add( Calendar.MONTH, -1 );
                        
                        childObjectBuilder.add( "payment_date", HierarchicalDataSource.this.dateFormat.format( cal.getTime() ) );
                        childObjectBuilder.add( "amount", 2000 + RNG.nextInt( 2000 ) );
                        
                        payChecksArrayBuilder.add( childObjectBuilder.build() );
                    }
                    
                    builder.add( "pay_checks", payChecksArrayBuilder.build() );
                }
                
                return builder.build();
            }
        };
    }
    
    public static void main( final String[] args )
    {
        for( final Iterator<JsonObject> itr = new HierarchicalDataSource( 5 ).iterator(); itr.hasNext(); )
        {
            System.out.println( itr.next().toString() );
        }
    }

}
