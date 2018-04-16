package com.prifender.encryption.service;

import java.security.GeneralSecurityException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public final class ResetApiController implements ResetApi
{
    @Autowired
    private EncryptionService encryptionService;

    @Override
    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ResponseEntity reset()
    {
        try
        {
            this.encryptionService.reset();
            
            return new ResponseEntity<Void>( HttpStatus.NO_CONTENT );
        }
        catch( final GeneralSecurityException e )
        {
            return new ResponseEntity<Problem>( new Problem().code( e.getClass().getSimpleName() ).message( e.getMessage() ), HttpStatus.BAD_REQUEST );
        }
    }

}
