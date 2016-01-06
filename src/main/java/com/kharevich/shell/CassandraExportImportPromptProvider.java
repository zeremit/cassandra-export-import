package com.kharevich.shell;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.PromptProvider;
import org.springframework.stereotype.Component;

/**
 * Created by zeremit on 1/6/16.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CassandraExportImportPromptProvider implements PromptProvider {
    public String getPrompt() {
        return "cqltool>";
    }

    public String getProviderName() {
        return "CassandraPromtProvider";
    }
}
