import os

import yaml

from datacontract.model.data_contract_specification import Server


def to_db2_soda_configuration(server: Server) -> str:
    """Serialize server config to soda configuration.


    ### Example:
        type: DB2
        host: 127.0.0.1
        port: '50000'
        username: simple
        password: simple_pass
        database: database
        schema: public
    """
    # with service account key, using an external json file
    soda_configuration = {
        f"data_source {server.type}": {
            "type": "db2",
            "host": server.host,
            "port": str(server.port),
            "username": os.getenv("DATACONTRACT_DB2_USERNAME", ""),
            "password": os.getenv("DATACONTRACT_DB2_PASSWORD", ""),
            "database": server.database,
            "schema": server.schema_,
        }
    }

    soda_configuration_str = yaml.dump(soda_configuration)
    return soda_configuration_str
