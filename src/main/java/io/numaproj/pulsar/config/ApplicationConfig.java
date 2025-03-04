package io.numaproj.pulsar.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "application")
public class ApplicationConfig {
    private final Environment environment;
    private String consumerPropertiesPath;
    private String producerPropertiesPath;
    private boolean isSource = false;
    private boolean isSink = false;

    public ApplicationConfig(Environment environment) {
        this.environment = environment;
    }

    @PostConstruct
    public void initialize() {
        // Determine if we're running as a source or sink based on properties
        if (consumerPropertiesPath != null && !consumerPropertiesPath.isEmpty()) {
            Path path = Paths.get(consumerPropertiesPath);
            if (Files.exists(path)) {
                isSource = true;
                log.info("Running as a source (consumer) with properties from: {}", consumerPropertiesPath);
            } else {
                log.warn("Consumer properties file not found at: {}", consumerPropertiesPath);
            }
        }

        if (producerPropertiesPath != null && !producerPropertiesPath.isEmpty()) {
            Path path = Paths.get(producerPropertiesPath);
            if (Files.exists(path)) {
                isSink = true;
                log.info("Running as a sink (producer) with properties from: {}", producerPropertiesPath);
            } else {
                log.warn("Producer properties file not found at: {}", producerPropertiesPath);
            }
        }

        // Validate configuration
        if (isSource && isSink) {
            log.warn("Both source and sink configurations detected. This may lead to unexpected behavior.");
        } else if (!isSource && !isSink) {
            log.warn("Neither source nor sink configuration detected. Application may not function correctly.");
        }
    }
}