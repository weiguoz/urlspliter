package org.hadoop.urlspliter;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by weiguo on 14-2-22.
 */
public class SLDExtractor {
    private Set<String> suffixes_;
    private Set<String> exceptions_;
    private static final SLDExtractor INSTACE = new SLDExtractor();

    public static SLDExtractor getInstace() {
        return INSTACE;
    }

    // http://svn.apache.org/repos/asf/httpcomponents/httpclient/trunk/httpclient/src/main/java/org/apache/http/impl/cookie/PublicSuffixListParser.java
    private SLDExtractor() {
        Collection<String> rules = new ArrayList();
        Collection<String> exceptions = new ArrayList();
        for (String s : TLD.get()) {
            if (s.length()==0 || s.startsWith("//")) continue; // entire lines can also be commented using //
            if (s.startsWith(".")) s = s.substring(1);         // A leading dot is optional
            // An exclamation mark (!) at the start of a rule marks an exception to a previous wildcard rule
            if (s.startsWith("!")) {
                s = s.substring(1);
                exceptions.add(s);
            } else {
                rules.add(s);
            }
        }
        suffixes_ = new HashSet<String>(rules);
        exceptions_ = new HashSet<String>(exceptions);
    }

    /**
     * Checks if the domain is a TLD.
     * @param domain
     * @return
     */
    public boolean isTLD(String domain) {
        if (domain.startsWith("."))
            domain = domain.substring(1);
        // An exception rule takes priority over any other matching rule.
        // Exceptions are ones that are not a TLD, but would match a pattern rule
        // e.g. bl.uk is not a TLD, but the rule *.uk means it is. Hence there is an exception rule
        // stating that bl.uk is not a TLD.
        if (this.exceptions_ != null && this.exceptions_.contains(domain))
            return false;
        if (this.suffixes_ == null)
            return false;
        if (this.suffixes_.contains(domain))
            return true;

        // Try patterns. ie *.jp means that boo.jp is a TLD
        int nextdot = domain.indexOf('.');
        if (nextdot == -1)
            return false;
        domain = "*" + domain.substring(nextdot);
        return this.suffixes_.contains(domain);
    }

    public static String getHost(String link) {
        String host="";
        try {
            if (!link.startsWith("http://") && !link.startsWith("https://") && !link.startsWith("ftp://"))
                link = "http://"+link;
            URL aURL = new URL(link);
            host = aURL.getHost();
        } catch (MalformedURLException e) {
        }
        return host;
    }

    public String extract(String link) {
        String domain = getHost(link);
        String last = domain;
        boolean gotSLD = false;
        do {
            if (isTLD(domain))
                return gotSLD?last:"EMPTY";
            gotSLD = true;
            last = domain;
            int nextDot = domain.indexOf(".");
            if (nextDot == -1)
                break;
            domain = domain.substring(nextDot+1);
        } while (domain.length() > 0);
        return "EMPTY";
    }
}
