import datetime
import logging
import re
from typing import Any, Dict, List
from venv import logger

from datacontract_specification.model import Quality
from open_data_contract_standard.model import CustomProperty, OpenDataContractStandard, SchemaProperty

from datacontract.imports.importer import Importer
from datacontract.lint.resources import read_resource
from datacontract.model.data_contract_specification import (
    DATACONTRACT_TYPES,
    Availability,
    DataContractSpecification,
    Field,
    Info,
    Model,
    Retention,
    Server,
    ServerRole,
    ServiceLevel,
    Terms,
)
from datacontract.model.exceptions import DataContractException


class OdcsImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        return import_odcs_v3_as_dcs(data_contract_specification, source)


def import_odcs_v3_as_dcs(
    data_contract_specification: DataContractSpecification, source: str
) -> DataContractSpecification:
    source_str = read_resource(source)
    odcs = parse_odcs_v3_from_str(source_str)
    return import_from_odcs(data_contract_specification, odcs)


def parse_odcs_v3_from_str(source_str):
    try:
        odcs = OpenDataContractStandard.from_string(source_str)
    except Exception as e:
        raise DataContractException(
            type="schema",
            name="Parse ODCS contract",
            reason=f"Failed to parse odcs contract from {source_str}",
            engine="datacontract",
            original_exception=e,
        )
    return odcs


def import_from_odcs(data_contract_specification: DataContractSpecification, odcs: OpenDataContractStandard):
    data_contract_specification.id = odcs.id
    data_contract_specification.info = import_info(odcs)
    data_contract_specification.servers = import_servers(odcs)
    data_contract_specification.terms = import_terms(odcs)
    data_contract_specification.servicelevels = import_servicelevels(odcs)
    data_contract_specification.models = import_models(odcs)
    data_contract_specification.tags = import_tags(odcs)
    return data_contract_specification


def import_info(odcs: Any) -> Info:
    info = Info()

    info.title = odcs.name if odcs.name is not None else ""

    if odcs.version is not None:
        info.version = odcs.version

    # odcs.description.purpose => datacontract.description
    if odcs.description is not None and odcs.description.purpose is not None:
        info.description = odcs.description.purpose

    # odcs.domain => datacontract.owner
    owner = get_owner(odcs.customProperties)
    if owner is not None:
        info.owner = owner

    # add dataProduct as custom property
    if odcs.dataProduct is not None:
        info.dataProduct = odcs.dataProduct

    # add tenant as custom property
    if odcs.tenant is not None:
        info.tenant = odcs.tenant

    return info


def import_server_roles(roles: List[Dict]) -> List[ServerRole] | None:
    if roles is None:
        return None
    result = []
    for role in roles:
        server_role = ServerRole()
        server_role.name = role.role
        server_role.description = role.description
        result.append(server_role)


def import_servers(odcs: OpenDataContractStandard) -> Dict[str, Server] | None:
    if odcs.servers is None:
        return None
    servers = {}
    for odcs_server in odcs.servers:
        server_name = odcs_server.server
        if server_name is None:
            logger.warning("Server name is missing, skipping server")
            continue

        server = Server()
        server.type = odcs_server.type
        server.description = odcs_server.description
        server.environment = odcs_server.environment
        server.format = odcs_server.format
        server.project = odcs_server.project
        server.dataset = odcs_server.dataset
        server.path = odcs_server.path
        server.delimiter = odcs_server.delimiter
        server.endpointUrl = odcs_server.endpointUrl
        server.location = odcs_server.location
        server.account = odcs_server.account
        server.database = odcs_server.database
        server.schema_ = odcs_server.schema_
        server.host = odcs_server.host
        server.port = odcs_server.port
        server.catalog = odcs_server.catalog
        server.topic = getattr(odcs_server, "topic", None)
        server.http_path = getattr(odcs_server, "http_path", None)
        server.token = getattr(odcs_server, "token", None)
        server.driver = getattr(odcs_server, "driver", None)
        server.roles = import_server_roles(odcs_server.roles)
        server.storageAccount = (
            re.search(r"(?:@|://)([^.]+)\.", odcs_server.location, re.IGNORECASE) if server.type == "azure" else None
        )
        servers[server_name] = server
    return servers


def import_terms(odcs: Any) -> Terms | None:
    if odcs.description is None:
        return None
    if odcs.description.usage is not None or odcs.description.limitations is not None or odcs.price is not None:
        terms = Terms()
        if odcs.description.usage is not None:
            terms.usage = odcs.description.usage
        if odcs.description.limitations is not None:
            terms.limitations = odcs.description.limitations
        if odcs.price is not None:
            terms.billing = f"{odcs.price.priceAmount} {odcs.price.priceCurrency} / {odcs.price.priceUnit}"

        return terms
    else:
        return None


def import_servicelevels(odcs: Any) -> ServiceLevel:
    # find the two properties we can map (based on the examples)
    sla_properties = odcs.slaProperties if odcs.slaProperties is not None else []
    availability = next((p for p in sla_properties if p.property == "generalAvailability"), None)
    retention = next((p for p in sla_properties if p.property == "retention"), None)

    if availability is not None or retention is not None:
        servicelevel = ServiceLevel()

        if availability is not None:
            value = availability.value
            if isinstance(value, datetime.datetime):
                value = value.isoformat()
            servicelevel.availability = Availability(description=value)

        if retention is not None:
            servicelevel.retention = Retention(period=f"{retention.value}{retention.unit}")

        return servicelevel
    else:
        return None


def get_server_type(odcs: OpenDataContractStandard) -> str | None:
    servers = import_servers(odcs)
    if servers is None or len(servers) == 0:
        return None
    # get first server from map
    server = next(iter(servers.values()))
    return server.type


def import_models(odcs: Any) -> Dict[str, Model]:
    custom_type_mappings = get_custom_type_mappings(odcs.customProperties)

    odcs_schemas = odcs.schema_ if odcs.schema_ is not None else []
    result = {}

    for odcs_schema in odcs_schemas:
        model_name = odcs_schema.name
        model = Model(
            description=odcs_schema.description,
            type=odcs_schema.physicalType,
            title=odcs_schema.name,
        )

        # Import fields with custom properties
        model.fields = import_fields(odcs_schema.properties, custom_type_mappings, server_type=None)

        # Import custom properties for the model
        if odcs_schema.customProperties is not None:
            for custom_property in odcs_schema.customProperties:
                model.add_custom_property(custom_property.property, custom_property.value)

        result[model_name] = model

    return result


def convert_quality_list(odcs_quality_list):
    """Convert a list of ODCS DataQuality objects to datacontract Quality objects"""
    quality_list = []

    if odcs_quality_list is not None:
        for odcs_quality in odcs_quality_list:
            quality = Quality(type=odcs_quality.type)

            if odcs_quality.description is not None:
                quality.description = odcs_quality.description
            if odcs_quality.query is not None:
                quality.query = odcs_quality.query
            if odcs_quality.mustBe is not None:
                quality.mustBe = odcs_quality.mustBe
            if odcs_quality.mustNotBe is not None:
                quality.mustNotBe = odcs_quality.mustNotBe
            if odcs_quality.mustBeGreaterThan is not None:
                quality.mustBeGreaterThan = odcs_quality.mustBeGreaterThan
            if odcs_quality.mustBeGreaterOrEqualTo is not None:
                quality.mustBeGreaterThanOrEqualTo = odcs_quality.mustBeGreaterOrEqualTo
            if odcs_quality.mustBeLessThan is not None:
                quality.mustBeLessThan = odcs_quality.mustBeLessThan
            if odcs_quality.mustBeLessOrEqualTo is not None:
                quality.mustBeLessThanOrEqualTo = odcs_quality.mustBeLessOrEqualTo
            if odcs_quality.mustBeBetween is not None:
                quality.mustBeBetween = odcs_quality.mustBeBetween
            if odcs_quality.mustNotBeBetween is not None:
                quality.mustNotBeBetween = odcs_quality.mustNotBeBetween
            if odcs_quality.engine is not None:
                quality.engine = odcs_quality.engine
            if odcs_quality.implementation is not None:
                quality.implementation = odcs_quality.implementation
            if odcs_quality.businessImpact is not None:
                quality.model_extra["businessImpact"] = odcs_quality.businessImpact
            if odcs_quality.dimension is not None:
                quality.model_extra["dimension"] = odcs_quality.dimension
            if odcs_quality.rule is not None:
                quality.model_extra["rule"] = odcs_quality.rule
            if odcs_quality.schedule is not None:
                quality.model_extra["schedule"] = odcs_quality.schedule
            if odcs_quality.scheduler is not None:
                quality.model_extra["scheduler"] = odcs_quality.scheduler
            if odcs_quality.severity is not None:
                quality.model_extra["severity"] = odcs_quality.severity
            if odcs_quality.method is not None:
                quality.model_extra["method"] = odcs_quality.method
            if odcs_quality.customProperties is not None:
                quality.model_extra["customProperties"] = []
                for item in odcs_quality.customProperties:
                    quality.model_extra["customProperties"].append(
                        {
                            "property": item.property,
                            "value": item.value,
                        }
                    )

            quality_list.append(quality)

    return quality_list


def import_field_config(odcs_property: SchemaProperty, server_type=None) -> Dict[str, Any]:
    config = {}
    if odcs_property.criticalDataElement is not None:
        config["criticalDataElement"] = odcs_property.criticalDataElement
    if odcs_property.encryptedName is not None:
        config["encryptedName"] = odcs_property.encryptedName
    if odcs_property.partitionKeyPosition is not None:
        config["partitionKeyPosition"] = odcs_property.partitionKeyPosition
    if odcs_property.partitioned is not None:
        config["partitioned"] = odcs_property.partitioned

    if odcs_property.customProperties is not None:
        for item in odcs_property.customProperties:
            config[item.property] = item.value

    physical_type = odcs_property.physicalType
    if physical_type is not None:
        if server_type == "postgres" or server_type == "postgresql":
            config["postgresType"] = physical_type
        elif server_type == "bigquery":
            config["bigqueryType"] = physical_type
        elif server_type == "snowflake":
            config["snowflakeType"] = physical_type
        elif server_type == "redshift":
            config["redshiftType"] = physical_type
        elif server_type == "sqlserver":
            config["sqlserverType"] = physical_type
        elif server_type == "databricks":
            config["databricksType"] = physical_type
        else:
            config["physicalType"] = physical_type

    return config


def has_composite_primary_key(odcs_properties: List[SchemaProperty]) -> bool:
    primary_keys = [prop for prop in odcs_properties if prop.primaryKey is not None and prop.primaryKey]
    return len(primary_keys) > 1


def import_fields(
    odcs_properties: List[SchemaProperty], custom_type_mappings: Dict[str, str], server_type
) -> Dict[str, Field]:
    result = {}

    if odcs_properties is None:
        return result

    for odcs_property in odcs_properties:
        field = Field(
            name=odcs_property.name,
            type=odcs_property.physicalType,
            description=odcs_property.description,
        )

        # Import custom properties for the field
        if odcs_property.customProperties is not None:
            for custom_property in odcs_property.customProperties:
                field.add_custom_property(custom_property.property, custom_property.value)

        result[odcs_property.name] = field

    return result


def map_type(odcs_type: str, custom_mappings: Dict[str, str]) -> str | None:
    if odcs_type is None:
        return None
    t = odcs_type.lower()
    if t in DATACONTRACT_TYPES:
        return t
    elif custom_mappings.get(t) is not None:
        return custom_mappings.get(t)
    else:
        return None


def get_custom_type_mappings(odcs_custom_properties: List[CustomProperty]) -> Dict[str, str]:
    result = {}
    if odcs_custom_properties is not None:
        for prop in odcs_custom_properties:
            if prop.property.startswith("dc_mapping_"):
                odcs_type_name = prop.property[11:]  # Changed substring to slice
                datacontract_type = prop.value
                result[odcs_type_name] = datacontract_type

    return result


def get_owner(odcs_custom_properties: List[CustomProperty]) -> str | None:
    if odcs_custom_properties is not None:
        for prop in odcs_custom_properties:
            if prop.property == "owner":
                return prop.value

    return None


def import_tags(odcs: OpenDataContractStandard) -> List[str] | None:
    if odcs.tags is None:
        return None
    return odcs.tags
