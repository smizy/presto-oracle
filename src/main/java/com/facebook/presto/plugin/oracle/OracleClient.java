/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.*;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Properties;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implementation of OracleClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 *
 * @author Marcelo Paes Rech
 */
public class OracleClient extends BaseJdbcClient {

    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config) throws SQLException {
        super(connectorId, config, "", connectionFactory(config));
        //the empty "" is to not use a quote to create queries
        // BaseJdbcClient already gets these properties
        // connectionProperties.setProperty("user", oracleConfig.getUser());
        // connectionProperties.setProperty("url", oracleConfig.getUrl());
        // connectionProperties.setProperty("password",
        // oracleConfig.getPassword());

    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config) {
        checkArgument(config.getConnectionUrl() != null, "Invalid JDBC URL for Oracle connector");
        checkArgument(config.getConnectionUser() != null, "Invalid JDBC User for Oracle connector");
        checkArgument(config.getConnectionPassword() != null, "Invalid JDBC Password for Oracle connector");
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("user", config.getConnectionUser());
        connectionProperties.setProperty("url", config.getConnectionUrl());
        connectionProperties.setProperty("password", config.getConnectionPassword());
        return new DriverConnectionFactory(new oracle.jdbc.OracleDriver(), config.getConnectionUrl(), connectionProperties);
    }


}
