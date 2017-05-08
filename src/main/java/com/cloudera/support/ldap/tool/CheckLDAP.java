package com.cloudera.support.ldap.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.LdapGroupsMapping;

public class CheckLDAP {

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    LdapGroupsMapping ldapResolver = new LdapGroupsMapping();

    Configuration config = new Configuration();

    config.set("hadoop.security.group.mapping.ldap.url", args[0]);
    config.set("hadoop.security.group.mapping.ldap.bind.user", args[1]);
    config.set("hadoop.security.group.mapping.ldap.bind.password", args[2]);
    config.set("hadoop.security.group.mapping.ldap.base", args[3]);
    config.set("hadoop.security.group.mapping.ldap.search.attr.member", args[4]);

    ldapResolver.setConf(config);

    for (String g : ldapResolver.getGroups(args[5])) {
      System.out.println(g);
    }

  }

}
