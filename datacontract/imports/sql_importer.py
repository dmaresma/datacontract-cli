import logging
import os

from simple_ddl_parser import parse_from_file

from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Field, Model, Server
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import ResultEnum


class SqlImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        return import_sql(data_contract_specification, self.import_format, source, import_args)


def import_sql(
    data_contract_specification: DataContractSpecification, format: str, source: str, import_args: dict = None
) -> DataContractSpecification:

    dialect = to_dialect(import_args)

    try:
        ddl = parse_from_file(source, group_by_type=True, encoding = "cp1252", output_mode = dialect )
    except Exception as e:
        logging.error(f"Error parsing SQL: {str(e)}")
        raise DataContractException(
            type="import",
            name=f"Reading source from {source}",
            reason=f"Error parsing SQL: {str(e)}",
            engine="datacontract",
            result=ResultEnum.error,
        )

    server_type: str | None = dialect
    if server_type is not None:
        data_contract_specification.servers[server_type] = Server(type=server_type)

    tables = ddl["tables"]

    for table in tables:
        if data_contract_specification.models is None:
            data_contract_specification.models = {}

        table_name = table["table_name"]

        fields = {}
        for column in table["columns"]:
            field = Field()
            field.type = map_type_from_sql(map_type_from_sql(column["type"]))
            if not column["nullable"]:
                field.required = True
            if column["unique"]:
                field.unique = True
            
            if column["size"] is not None and column["size"] and not isinstance(column["size"], tuple):
                field.maxLength = column["size"]
            elif isinstance(column["size"], tuple):
                field.precision = column["size"][0]
                field.scale = column["size"][1]

            field.description = column["comment"][1:-1] if column.get("comment") else None
            field.required = column["nullable"]
            if column.get("with_tag"):
                field.tags = ", ".join(column["with_tag"])
            if column.get("with_masking_policy"):
                field.classification = ", ".join(column["with_masking_policy"]) 
            if column.get("generated"):
                field.examples = str(column["generated"])
            field.unique = column["unique"]

            fields[column["name"]] = field
            
            if table.get("constraints"):
                if table["constraints"].get("primary_key"):                   
                    for primary_key in table["constraints"]["primary_key"]["columns"]:
                        if primary_key in fields:
                            fields[primary_key].unique = True
                            fields[primary_key].required = True
                            fields[primary_key].primaryKey = True

            table_description = table["comment"][1:-1] if  table.get("comment") else None
            table_tags = table["with_tag"][1:-1] if  table.get("with_tag") else None
            
        data_contract_specification.models[table_name] = Model(
            type="table",
            description=table_description,
            tags=table_tags,
            fields=fields,
        )

    return data_contract_specification

def to_dialect(import_args: dict) -> str | None:
    if import_args is None:
        return None
    if "dialect" not in import_args:
        return None
    dialect = import_args.get("dialect")
    return dialect

def to_col_type(column, dialect):
    col_type_kind = column.args["kind"]
    if col_type_kind is None:
        return None

    return col_type_kind.sql(dialect)

def to_col_type_normalized(column):
    col_type = column.args["kind"].this.name
    if col_type is None:
        return None
    return col_type.lower()

def map_type_from_sql(sql_type: str):
    if sql_type is None:
        return None

    sql_type_normed = sql_type.lower().strip()

    if sql_type_normed.startswith("varchar"):
        return "string"
    elif sql_type_normed.startswith("char"):
        return "string"
    elif sql_type_normed.startswith("string"):
        return "string"
    elif sql_type_normed.startswith("nchar"):
        return "string"
    elif sql_type_normed.startswith("text"):
        return "string"
    elif sql_type_normed.startswith("nvarchar"):
        return "string"
    elif sql_type_normed.startswith("ntext"):
        return "string"
    elif sql_type_normed.startswith("number"):
        return "decimal"
    elif sql_type_normed.startswith("int"):
        return "decimal"
    elif sql_type_normed.startswith("bigint"):
        return "long"
    elif sql_type_normed.startswith("tinyint"):
        return "decimal"
    elif sql_type_normed.startswith("smallint"):
        return "decimal"
    elif sql_type_normed.startswith("float"):
        return "float"
    elif sql_type_normed.startswith("decimal"):
        return "decimal"
    elif sql_type_normed.startswith("numeric"):
        return "decimal"
    elif sql_type_normed.startswith("bool"):
        return "boolean"
    elif sql_type_normed.startswith("bit"):
        return "boolean"
    elif sql_type_normed.startswith("binary"):
        return "bytes"
    elif sql_type_normed.startswith("varbinary"):
        return "bytes"
    elif sql_type_normed == "date":
        return "date"
    elif sql_type_normed == "time":
        return "string"
    elif sql_type_normed == "timestamp":
        return "timestamp_ntz"
    elif (
        sql_type_normed == "timestamptz"
        or sql_type_normed == "timestamp_tz"
        or sql_type_normed == "timestamp with time zone"
    ):
        return "timestamp_tz"
    elif sql_type_normed == "timestampntz" or sql_type_normed == "timestamp_ntz":
        return "timestamp_ntz"
    elif sql_type_normed == "smalldatetime":
        return "timestamp_ntz"
    elif sql_type_normed == "datetime":
        return "timestamp_ntz"
    elif sql_type_normed == "datetime2":
        return "timestamp_ntz"
    elif sql_type_normed == "datetimeoffset":
        return "timestamp_tz"
    elif sql_type_normed == "uniqueidentifier":  # tsql
        return "string"
    elif sql_type_normed == "json":
        return "string"
    elif sql_type_normed == "xml":  # tsql
        return "string"
    else:
        return "variant"


def read_file(path):
    if not os.path.exists(path):
        raise DataContractException(
            type="import",
            name=f"Reading source from {path}",
            reason=f"The file '{path}' does not exist.",
            engine="datacontract",
            result=ResultEnum.error,
        )
    with open(path, "r") as file:
        file_content = file.read()
    return file_content
