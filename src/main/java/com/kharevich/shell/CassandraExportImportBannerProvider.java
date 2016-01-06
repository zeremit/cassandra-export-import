package com.kharevich.shell;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.BannerProvider;
import org.springframework.stereotype.Component;

/**
 * Created by zeremit on 1/6/16.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CassandraExportImportBannerProvider implements BannerProvider {
    public String getBanner() {
        return "Hello";
    }

    public String getVersion() {
        return "1.0";
    }

    public String getWelcomeMessage() {
        return "Welcom to Cassandra Export Import Tool";
    }

    public String getProviderName() {
        return "CassandraBannerProvider";
    }
}
