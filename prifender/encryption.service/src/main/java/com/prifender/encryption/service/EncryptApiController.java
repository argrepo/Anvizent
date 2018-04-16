package com.prifender.encryption.service;

import java.security.GeneralSecurityException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
public final class EncryptApiController implements EncryptApi
{
    @Autowired
    private EncryptionService encryptionService;

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity encrypt( @RequestBody final String value )
    {
        try
        {
            return new ResponseEntity<String>( this.encryptionService.encrypt( value ), HttpStatus.OK );
        }
        catch( final GeneralSecurityException e )
        {
            return new ResponseEntity<Problem>( new Problem().code( e.getClass().getSimpleName() ).message( e.getMessage() ), HttpStatus.BAD_REQUEST );
        }
    }

}
