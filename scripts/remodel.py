from __future__ import annotations

import json
import os
from collections.abc import Sequence
from contextlib import suppress
from copy import deepcopy
from io import StringIO
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Callable, cast
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import jsonpointer  # type: ignore
import oapi
import yaml  # type: ignore
from sob.model import serialize

PROJECT_PATH: Path = Path(__file__).absolute().parent.parent
OPENAPI_PATH: Path = PROJECT_PATH / "openapi"

# Sub-package names corresponding to each OpenAPI document
PROVIDER_DATA_V1: str = "provider_data/v1"

# Map API package names to OpenAPI document URLs
OPENAPI_DOCUMENT_URL: dict[str, str] = {
    PROVIDER_DATA_V1: "https://data.cms.gov/provider-data/api/1"
}
# Map API package names to model modules' paths, relative to the project root
MODEL_PY: dict[str, Path] = {
    PROVIDER_DATA_V1: PROJECT_PATH / f"src/cmsgov/{PROVIDER_DATA_V1}/model.py"
}
# Map API package names to client modules' paths, relative to the project root
CLIENT_PY: dict[str, Path] = {
    PROVIDER_DATA_V1: PROJECT_PATH / f"src/cmsgov/{PROVIDER_DATA_V1}/client.py"
}

STRING_SCHEMA: oapi.oas.Schema = oapi.oas.Schema(type_="string")
INTEGER_SCHEMA: oapi.oas.Schema = oapi.oas.Schema(type_="integer")
BOOLEAN_SCHEMA: oapi.oas.Schema = oapi.oas.Schema(type_="boolean")
OBJECT_SCHEMA: oapi.oas.Schema = oapi.oas.Schema(type_="object")
ARRAY_SCHEMA: oapi.oas.Schema = oapi.oas.Schema(type_="array")

if TYPE_CHECKING:
    import sob


def fix_openapi_data(data: str) -> str:
    """
    Fix errors in an Open API document which prevent the document from being
    parsed, if needed.

    Returns:
        Parseable JSON/YAML data.
    """
    return data


def get_openapi(openapi_document_path: str | Path) -> oapi.oas.OpenAPI:
    """
    Load and parse a locally saved Open API document.
    """
    openapi_document_path_lowercase: str = (
        str(openapi_document_path.absolute()).lower()
        if isinstance(openapi_document_path, Path)
        else openapi_document_path.lower()
    )
    openapi_document_io: IO[str]
    openapi_document_json: str
    openapi_document_dict: dict[str, Any]
    with open(openapi_document_path) as openapi_document_io:
        openapi_document_json = openapi_document_io.read()
    openapi_document_json = fix_openapi_data(openapi_document_json)
    openapi_document_io = StringIO(openapi_document_json)
    if openapi_document_path_lowercase.endswith((".yaml", ".yml")):
        openapi_document_dict = yaml.safe_load(openapi_document_io)
    else:
        openapi_document_dict = json.load(openapi_document_io)
    return oapi.oas.OpenAPI(openapi_document_dict)


def fix_provider_data_openapi(
    openapi_document: oapi.oas.OpenAPI,
) -> None:
    """
    Modify the Open API document to correct discrepancies between the
    document and actual API behavior/responses/etc.

    We script these fixes in order to be able to re-generate the client
    and model if/when the source document is modified, without losing these
    fixes we've identified as necessary.
    """
    # Add a server
    if not openapi_document.servers:
        openapi_document.servers = (
            oapi.oas.Server(
                url="https://data.cms.gov/provider-data/api/1",
                description="CMS Provider Data API V1",
            ),
        )
    # Create a model for an array of datasets
    if TYPE_CHECKING:
        assert openapi_document.components
    schemas: oapi.oas.Schemas = cast(
        oapi.oas.Schemas,
        cast(oapi.oas.Components, openapi_document.components).schemas,
    )
    schemas["datasets"] = oapi.oas.Schema(
        type_="array",
        items=oapi.oas.Reference(ref="#/components/schemas/dataset"),
        description="An array of datasets.",
    )
    # Remove unnecessary path prefixes
    openapi_document_paths: oapi.oas.Paths = cast(
        oapi.oas.Paths, openapi_document.paths
    )
    path: str
    for path in tuple(openapi_document_paths.keys()):
        if not path.startswith("/provider-data/api/1/"):
            raise ValueError(path)
        openapi_document_paths[path[20:]] = openapi_document_paths.pop(path)
    # Add the metastore schemas dataset endpoint for retrieving *all* datasets
    # by copying the path for retrieving a single dataset
    metastore_schemas_dataset_items: oapi.oas.PathItem
    metastore_schemas_dataset_items = openapi_document_paths[
        "/metastore/schemas/dataset/items"
    ] = deepcopy(
        openapi_document_paths["/metastore/schemas/dataset/items/{identifier}"]
    )
    get_metastore_schemas_dataset_items: oapi.oas.Operation = cast(
        oapi.oas.Operation, metastore_schemas_dataset_items.get
    )
    get_metastore_schemas_dataset_items.summary = "Get all datasets."
    # Filter out the copied `datasetUuid` parameter
    parameter: oapi.oas.Parameter
    get_metastore_schemas_dataset_items.parameters = tuple(
        filter(
            lambda parameter: (
                (not isinstance(parameter, oapi.oas.Reference))
                or parameter.ref != "#/components/parameters/datasetUuid"
            ),
            get_metastore_schemas_dataset_items.parameters or (),
        )
    )
    # Change the response type from dataset to datasets
    if TYPE_CHECKING:
        assert get_metastore_schemas_dataset_items.responses
    get_metastore_schemas_dataset_items.responses["200"].content[
        "application/json"
    ].schema.ref = "#/components/schemas/datasets"
    # Add missing dataset attributes
    schemas["dataset"].properties["landingPage"] = STRING_SCHEMA
    # Add missing datastore query resource properties
    datastore_query_resource_schema_properties: oapi.oas.Properties = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/components/schemas/datastoreQuery/properties/resources/items"
            "/properties",
        )
    )
    datastore_query_resource_schema_properties["id"] = STRING_SCHEMA
    # Align GET and POST responses for
    # /datastore/query and /datastore/query/download
    get_datastore_query_parameters: sob.abc.Array = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1datastore~1query/get/parameters",
        )
    )
    get_datastore_query_download_parameters: sob.abc.Array = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1datastore~1query~1download/get/parameters",
        )
    )
    get_datastore_query_distribution_id_parameters: sob.abc.Array = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1datastore~1query~1{distributionId}/get/parameters",
        )
    )
    get_datastore_query_distribution_id_download_parameters: sob.abc.Array = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1datastore~1query~1{distributionId}~1download"
            "/get/parameters",
        )
    )
    get_datastore_query_dataset_id_index_parameters: sob.abc.Array = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1datastore~1query~1{datasetId}~1{index}/get/parameters",
        )
    )
    get_datastore_query_dataset_id_index_download_parameters: sob.abc.Array = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1datastore~1query~1{datasetId}~1{index}~1download"
            "/get/parameters",
        )
    )
    parameter_name: str
    for parameter_name in (
        "resources",
        "properties",
        "conditions",
        "joins",
        "groupings",
        "sorts",
    ):
        # Create array parameters for GET request datastore queries
        parameter = oapi.oas.Parameter(
            name=parameter_name,
            in_="query",
            required=False,
            style="deepObject",
            schema=oapi.oas.Reference(
                ref=(
                    "#/components/schemas/datastoreQuery/properties/"
                    f"{parameter_name}"
                )
            ),
            description=jsonpointer.resolve_pointer(
                openapi_document,
                "/components/schemas/datastoreQuery/properties/"
                f"{parameter_name}",
            ).description,
        )
        get_datastore_query_download_parameters.append(parameter)
        get_datastore_query_parameters.append(parameter)
        get_datastore_query_distribution_id_parameters.append(parameter)
        get_datastore_query_distribution_id_download_parameters.append(
            parameter
        )
        get_datastore_query_dataset_id_index_parameters.append(parameter)
        get_datastore_query_dataset_id_index_download_parameters.append(
            parameter
        )
    # Fix datastore/query descriptions to reflect above modifications
    operation: oapi.oas.Operation
    operation_pointer: str
    for operation_pointer in (
        "/paths/~1datastore~1query/get",
        "/paths/~1datastore~1query~1download/get",
        "/paths/~1datastore~1query~1{distributionId}/get",
        "/paths/~1datastore~1query~1{distributionId}~1download/get",
        "/paths/~1datastore~1query~1{datasetId}~1{index}/get",
        "/paths/~1datastore~1query~1{datasetId}~1{index}~1download/get",
    ):
        operation = jsonpointer.resolve_pointer(
            openapi_document,
            operation_pointer,
        )
        # If the description just indicates we should reference the POST
        # operation, remove it.
        if operation.description and ("POST" in operation.description):
            operation.description = None
    # Fix component parameters
    parameters: oapi.oas.Parameters = cast(
        oapi.oas.Parameters,
        cast(oapi.oas.Components, openapi_document.components).parameters,
    )
    cast(
        oapi.oas.Schema, parameters["datastoreDistributionIndex"].schema
    ).type_ = "integer"
    # Fix responses
    responses: oapi.oas.Responses = cast(
        oapi.oas.Responses,
        cast(oapi.oas.Components, openapi_document.components).responses,
    )
    json_or_csv_query_ok_response_schema: oapi.oas.Schema = (
        responses["200JsonOrCsvQueryOk"].content["application/json"].schema
    )
    json_or_csv_query_ok_response_schema_properties: oapi.oas.Properties = (
        cast(
            oapi.oas.Properties,
            json_or_csv_query_ok_response_schema.properties,
        )
    )
    json_or_csv_query_ok_response_schema_properties["schema"] = (
        oapi.oas.Schema(
            any_of=(
                json_or_csv_query_ok_response_schema_properties["schema"],
                oapi.oas.Schema(type_="array"),
            )
        )
    )
    # Fix search GET response data types
    search_get_response_schema_properties: oapi.oas.Properties = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1search/get/responses/200/content/application~1json"
            "/schema/properties",
        )
    )
    # The total property can be either a string or an integer
    total_type: oapi.oas.Schema = cast(
        oapi.oas.Schema,
        search_get_response_schema_properties["total"],
    )
    total_type.any_of = (STRING_SCHEMA, INTEGER_SCHEMA)
    total_type.type_ = None
    # The results property can be an array
    results_type: oapi.oas.Schema = cast(
        oapi.oas.Schema,
        search_get_response_schema_properties["results"],
    )
    results_type.any_of = (OBJECT_SCHEMA, ARRAY_SCHEMA)
    results_type.type_ = None
    # Fix search facets GET response data types
    search_facets_get_response_schema_properties: oapi.oas.Properties = (
        jsonpointer.resolve_pointer(
            openapi_document,
            "/paths/~1search~1facets/get/responses/200/content"
            "/application~1json/schema/properties",
        )
    )
    search_facets_get_response_schema_properties["results"] = (
        search_get_response_schema_properties["results"]
    )
    search_facets_get_response_schema_properties["total"] = (
        search_get_response_schema_properties["total"]
    )
    # Fix facets items data type
    facets_items_properties: oapi.oas.Properties = jsonpointer.resolve_pointer(
        openapi_document, "/components/schemas/facets/items/properties"
    )
    facets_items_properties_total_schema: oapi.oas.Schema = (
        facets_items_properties["total"]
    )
    facets_items_properties_total_schema.any_of = (
        STRING_SCHEMA,
        INTEGER_SCHEMA,
    )
    facets_items_properties_total_schema.type_ = None


def download(
    url: str,
    path: str | Path,
) -> Path:
    """
    Download a file
    """
    response: IO[bytes]
    with urlopen(url) as response:  # noqa: S310
        data: str = response.read().decode("utf-8", errors="ignore")
    if isinstance(path, str):
        path = Path(path).absolute()
    if path.suffix.lower() not in (".json", ".yaml", ".yml"):
        try:
            json.loads(data)
        except json.JSONDecodeError:
            try:
                yaml.safe_load(data)
            except yaml.YAMLError:
                digit: int
                if data.startswith(
                    (
                        "{",
                        "[",
                        '"',
                        *map(str, range(10)),
                    )
                ):
                    path = Path(f"{path}.json")
                else:
                    path = Path(f"{path}.yaml")
            else:
                path = Path(f"{path}.yaml")
        else:
            path = Path(f"{path}.json")
    if path.suffix.lower() == ".json":
        with suppress(json.JSONDecodeError):
            data = json.dumps(json.loads(data), indent=4)
    with open(path, "w") as file:
        file.write(data)
    return Path(path) if isinstance(path, str) else path


def update_openapi_original(name: str, format_: str | None = None) -> Path:
    if not format_:
        # Try to determine the format from the URL
        url: str = OPENAPI_DOCUMENT_URL[name]
        base_file_name: str
        # Use the file extension, if there is one in the URL
        base_file_name, format_ = (
            OPENAPI_DOCUMENT_URL[name]
            .rstrip("/")
            .rpartition("/")[-1]
            .rpartition(".")[::2]
        )
        if base_file_name:
            format_ = format_.lower()
        else:
            # If there was no "." in the file nane, try parsing the URL query
            # string and looking for a "format" or "format_" parameter
            query_str: str | None = urlparse(url).query
            if query_str:
                query: dict[str, list[str]] = parse_qs(query_str)
                format_ = query.get("format", query.get("format_", (None,)))[0]
            else:
                format_ = None
    extension: str = (
        f".{format_}" if format_ in ("json", "yaml", "yml") else ""
    )
    os.makedirs(OPENAPI_PATH / name, exist_ok=True)
    return download(
        OPENAPI_DOCUMENT_URL[name],
        OPENAPI_PATH / name / f"original{extension}",
    )


def update_model(
    name: str,
    fix_openapi: (
        Callable[
            [
                oapi.oas.OpenAPI,
            ],
            None,
        ]
        | None
    ) = None,
) -> Path | None:
    """
    Refresh (or initialize) the client's data model from the source Open API
    document.

    Parameters:
        name: The name of the Open API document
        fix_openapi: A callback function to fix the Open API document
            prior to generating the model
    """
    original: Path = update_openapi_original(name)
    if not original.exists():
        raise FileNotFoundError(str(original))
    open_api: oapi.oas.OpenAPI = get_openapi(original)
    if fix_openapi is not None:
        fix_openapi(open_api)
    fixed: Path = OPENAPI_PATH / name / "fixed.json"
    fixed_io: IO[str]
    with open(
        fixed,
        "w",
    ) as fixed_io:
        fixed_io.write(serialize(open_api, indent=4))
    model_py: Path = MODEL_PY[name]
    oapi.write_model_module(
        model_py,
        open_api=open_api,
    )
    return model_py


def update_client(name: str) -> None:
    open_api: oapi.oas.OpenAPI = get_openapi(
        OPENAPI_PATH / name / "fixed.json"
    )
    url: str = ""
    if open_api.servers:
        url = cast(str, cast(Sequence, open_api.servers)[0].url)
    client_py: Path = CLIENT_PY[name]
    model_py: Path = MODEL_PY[name]
    oapi.write_client_module(
        client_py,
        open_api=open_api,
        model_path=model_py,
        include_init_parameters=(
            "url",
            "user",
            "password",
            # "bearer_token",
            # "api_key",
            # "api_key_in",
            # "api_key_name",
            # "oauth2_client_id",
            # "oauth2_client_secret",
            # "oauth2_token_url",
            # "oauth2_username",
            # "oauth2_password",
            # "oauth2_authorization_url",
            # "oauth2_token_url",
            # "oauth2_scope",
            # "oauth2_refresh_url",
            # "oauth2_flows",
            # "open_id_connect_url",
            # "headers",
            "timeout",
            "retry_number_of_attempts",
            # "retry_for_errors",
            # "retry_hook",
            # "verify_ssl_certificate",
            "logger",
            "echo",
        ),
        init_parameter_defaults={
            "url": url,
            "retry_number_of_attempts": 3,
        },
    )


def main() -> None:
    update_model(PROVIDER_DATA_V1, fix_provider_data_openapi)
    update_client(PROVIDER_DATA_V1)


if __name__ == "__main__":
    main()
