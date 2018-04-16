package com.prifender.des.adapter.relational.sqlserverkerbores;

public class SqlServerKRBConfiguration {
	
	/*public static  String krb5_ini = "[libdefaults] \n " +

			 "default_realm = ANVIZENT.LOCAL\n "+
			
			 "default_tkt_enctypes = aes256-cts-hmac-sha1-96 rc4-hmac des-cbc-crc des-cbc-md5 \n " +

			 "default_tgs_enctypes = aes256-cts-hmac-sha1-96 rc4-hmac des-cbc-crc des-cbc-md5 \n " +

			 "permitted_enctypes =  aes256-cts-hmac-sha1-96 rc4-hmac des-cbc-crc des-cbc-md5 \n " +

			 "ticket_lifetime = 600 \n " +

			 "kdc_timesync = 1 \n " +

			 "ccache_type = 4 \n " +

			 "[realms] \n " +

			 "ANVIZENT.LOCAL = { \n " +

			 "kdc = 192.168.0.117 \n " +

			 "admin_server = WIN-2016ANVAD.ANVIZENT.LOCAL \n " +

			 "default_domain = anvizent.local \n " +

			 "} \n " +

			 "[domain_realm] \n " +

			 ".anvizent.local = .ANVIZENT.LOCAL \n " +

			 "anvizent.local = ANVIZENT.LOCAL  \n " +

			 "[appdefaults] \n " +

			 "autologin = true \n " +

			 "forward = true \n " +

			 "forwardable = true \n " +

			 "encrypt = true \n ";*/
	  final static String KRB_CONFIG =
	           "[libdefaults]\n" +
	                   "\tdefault_realm = ANVIZENT.LOCAL\n" +
	                   "\t\n" +
	                   "[domain_realm]\n" +
	                   "    .anvizent.local = ANVIZENT.LOCAL\n" +
	                   "    anvizent.local = ANVIZENT.LOCAL\n" +
	                   " \n" +
	                   "[realms]\n" +
	                   " ANVIZENT.LOCAL = {\n" +
	                   "\tadmin_server = 192.168.0.117\n" +
	                   "\tkdc = 192.168.0.117\n" +
	                   " }";
	static String KRB_LOGIN_CONFIG = "login {"+
	
			 "com.sun.security.auth.module.Krb5LoginModule required \n " +

			 "doNotPrompt=true \n" +

			 "useTicketCache=true; \n" +

			 "};";

}
