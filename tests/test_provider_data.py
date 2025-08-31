from __future__ import annotations

from itertools import islice
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError

import pytest
import sob

from cmsgov.provider_data.v1.model import (
    DatastoreImportsPostRequest,
    DatastoreQuery,
    DatastoreQueryCondition,
    DatastoreQueryConditions,
    DatastoreQueryResource,
    DatastoreQueryResources,
    DatastoreResourceQuery,
    DatastoreSqlGetResponse,
    HarvestPlan,
    HarvestPlanExtract,
    HarvestPlanLoad,
    HarvestRunsPostRequest,
    JsonOrCsvQueryOkResponse,
    MetastoreRevision,
    MetastoreSchemaRevisionPostRequest,
    MetastoreSchemasDatasetItemsIdentifierPatchRequest,
    MetastoreSchemasDatasetItemsPatchRequest,
    MetastoreSchemasSchemaIdItemsGetResponse,
    SearchFacetsGetResponse,
    SearchGetResponse,
)

if TYPE_CHECKING:
    from cmsgov.provider_data.v1.client import Client
    from cmsgov.provider_data.v1.model import (
        Dataset,
        Datasets,
        DatastoreImportGetResponse,
    )


def test_client_get_metastore_schemas_dataset_items(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items
    """
    datasets: Datasets = client.get_metastore_schemas_dataset_items()
    sob.model.validate(datasets)


def test_client_get_metastore_schemas_dataset_items_identifier(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{identifier}
    """
    dataset: Dataset = client.get_metastore_schemas_dataset_items_identifier(
        identifier="0ba7-2cb0"
    )
    sob.model.validate(dataset)


def test_put_metastore_schemas_dataset_items_identifier(
    client: Client,
) -> None:
    """
    Test a PUT request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{identifier}
    """
    dataset: Dataset = client.get_metastore_schemas_dataset_items_identifier(
        identifier="0ba7-2cb0"
    )
    try:
        client.put_metastore_schemas_dataset_items_identifier(
            dataset, "0ba7-2cb0"
        )
    except HTTPError as error:
        if error.code != 501:
            raise
    else:
        message: str = (
            "Attempting to create a dataset unauthenticated should "
            "result in a 501 error"
        )
        raise RuntimeError(message)


def test_patch_metastore_schemas_dataset_items_identifier(
    client: Client,
) -> None:
    """
    Test a PATCH request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{identifier}
    """
    dataset: Dataset = client.get_metastore_schemas_dataset_items_identifier(
        identifier="0ba7-2cb0"
    )
    try:
        client.patch_metastore_schemas_dataset_items_identifier(
            MetastoreSchemasDatasetItemsIdentifierPatchRequest(
                title=f"{dataset.title} Updated"
            ),
            "0ba7-2cb0",
        )
    except HTTPError as error:
        if error.code != 501:
            raise
    else:
        message: str = (
            "Attempting to update a dataset unauthenticated should "
            "result in a 501 error"
        )
        raise RuntimeError(message)


def test_put_metastore_schemas_dataset_items(
    client: Client,
) -> None:
    """
    Test a PUT request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items
    """
    dataset: Dataset = client.get_metastore_schemas_dataset_items_identifier(
        identifier="0ba7-2cb0"
    )
    try:
        client.put_metastore_schemas_dataset_items(dataset, "0ba7-2cb0")
    except HTTPError as error:
        if error.code != 501:
            raise
    else:
        message: str = (
            "Attempting to create a dataset unauthenticated should "
            "result in a 501 error"
        )
        raise RuntimeError(message)


def test_patch_metastore_schemas_dataset_items(
    client: Client,
) -> None:
    """
    Test a PATCH request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items
    """
    dataset: Dataset = client.get_metastore_schemas_dataset_items_identifier(
        identifier="0ba7-2cb0"
    )
    try:
        client.patch_metastore_schemas_dataset_items(
            MetastoreSchemasDatasetItemsPatchRequest(
                title=f"{dataset.title} Updated"
            ),
            "0ba7-2cb0",
        )
    except HTTPError as error:
        if error.code != 501:
            raise
    else:
        message: str = (
            "Attempting to update a dataset unauthenticated should "
            "result in a 501 error"
        )
        raise RuntimeError(message)


def test_client_delete_datastore_imports_identifier(
    client: Client,
) -> None:
    """
    Test a DELETE request to
    https://data.cms.gov/provider-data/api/1//datastore/imports/{identifier}
    """
    datasets: Datasets = client.get_metastore_schemas_dataset_items()
    dataset: Dataset
    for dataset in datasets:
        if TYPE_CHECKING:
            assert isinstance(dataset.identifier, str)
        try:
            client.delete_datastore_imports_identifier(dataset.identifier)
        except HTTPError as error:
            if error.code != 501:
                raise
            break
        else:
            message: str = (
                "Attempting to delete a public dataset should result in an "
                "error"
            )
            raise RuntimeError(message)


def test_client_get_datastore_imports(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/imports
    """
    try:
        client.get_datastore_imports()
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to retrieve all datastores unauthenticated should "
            "result in a 401 error"
        )
        raise RuntimeError(message)


def test_client_post_datastore_imports(
    client: Client,
) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/datastore/imports
    """
    try:
        client.post_datastore_imports(DatastoreImportsPostRequest())
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to retrieve all datastores unauthenticated should "
            "result in a 401 error"
        )
        raise RuntimeError(message)


def test_client_get_datastore_imports_identifier(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/imports/{identifier}
    """
    response: DatastoreImportGetResponse = (
        client.get_datastore_imports_identifier(
            identifier="1ee2fea0-00a3-58f4-8717-89b3cd62e442"
        )
    )
    # Validating an empty datastore is meaningless, make sure there are rows
    assert response.num_of_rows
    sob.model.validate(response)


def test_client_post_datastore_query(
    client: Client,
) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/datastore/query
    """
    query: DatastoreQuery = DatastoreQuery(
        conditions=DatastoreQueryConditions(
            [
                DatastoreQueryCondition(
                    resource="t",
                    property_="record_number",
                    value="1",
                    operator=">",
                )
            ]
        ),
        limit=3,
        resources=DatastoreQueryResources(
            [
                DatastoreQueryResource(
                    id_="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
                    alias="t",
                )
            ]
        ),
        format_="json",
    )
    response: JsonOrCsvQueryOkResponse | str = client.post_datastore_query(
        query
    )
    # The result should be an object, since we requested JSON
    assert isinstance(response, JsonOrCsvQueryOkResponse)
    sob.model.validate(response)
    query.format_ = "csv"
    response = client.post_datastore_query(query)
    assert isinstance(response, str)


def test_client_post_datastore_query_download(
    client: Client,
) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/datastore/query/download
    """
    query: DatastoreQuery = DatastoreQuery(
        conditions=DatastoreQueryConditions(
            [
                DatastoreQueryCondition(
                    resource="t",
                    property_="record_number",
                    value="1",
                    operator=">",
                )
            ]
        ),
        limit=3,
        resources=DatastoreQueryResources(
            [
                DatastoreQueryResource(
                    id_="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
                    alias="t",
                )
            ]
        ),
        format_="csv",
    )
    response: str = client.post_datastore_query_download(query)
    assert isinstance(response, str)


def test_client_get_datastore_query(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/query
    """
    kwargs: dict[str, Any] = {
        "conditions": DatastoreQueryConditions(
            [
                DatastoreQueryCondition(
                    resource="t",
                    property_="record_number",
                    value="1",
                    operator=">",
                )
            ]
        ),
        "limit": 3,
        "resources": DatastoreQueryResources(
            [
                DatastoreQueryResource(
                    id_="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
                    alias="t",
                )
            ]
        ),
    }
    response: JsonOrCsvQueryOkResponse | str = client.get_datastore_query(
        format_="csv", **kwargs
    )
    assert isinstance(response, str)
    response = client.get_datastore_query(format_="json", **kwargs)
    # The result should be an object, since we requested JSON
    assert isinstance(response, JsonOrCsvQueryOkResponse)
    # There should be results returned, otherwise the test is meaningless
    assert response.results
    sob.model.validate(response)


def test_client_get_datastore_query_download(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/query/download
    """
    kwargs: dict[str, Any] = {
        "conditions": DatastoreQueryConditions(
            [
                DatastoreQueryCondition(
                    resource="t",
                    property_="record_number",
                    value="1",
                    operator=">",
                )
            ]
        ),
        "limit": 3,
        "resources": DatastoreQueryResources(
            [
                DatastoreQueryResource(
                    id_="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
                    alias="t",
                )
            ]
        ),
    }
    response: JsonOrCsvQueryOkResponse | str = (
        client.get_datastore_query_download(format_="csv", **kwargs)
    )
    assert isinstance(response, str)


def test_client_get_datastore_query_distribution_id(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/query/{distributionId}
    """
    response: JsonOrCsvQueryOkResponse | str = (
        client.get_datastore_query_distribution_id(
            distribution_id="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
        )
    )
    assert isinstance(response, JsonOrCsvQueryOkResponse)
    sob.model.validate(response)
    response = client.get_datastore_query_distribution_id(
        distribution_id="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
        format_="csv",
    )
    assert isinstance(response, str)


def test_client_post_datastore_query_distribution_id(
    client: Client,
) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/datastore/query/{distributionId}
    """
    response: JsonOrCsvQueryOkResponse | str = (
        client.post_datastore_query_distribution_id(
            DatastoreResourceQuery(),
            distribution_id="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
        )
    )
    assert isinstance(response, JsonOrCsvQueryOkResponse)
    sob.model.validate(response)
    response = client.post_datastore_query_distribution_id(
        DatastoreResourceQuery(format_="csv"),
        distribution_id="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
    )
    assert isinstance(response, str)


def test_client_get_datastore_query_distribution_id_download(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/query/{distributionId}/download
    """
    response: JsonOrCsvQueryOkResponse | str = (
        client.get_datastore_query_distribution_id_download(
            distribution_id="1ee2fea0-00a3-58f4-8717-89b3cd62e442",
            format_="csv",
        )
    )
    assert isinstance(response, str)


def test_client_get_datastore_query_dataset_id_index(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/query/{datasetId}/{index}
    """
    response: JsonOrCsvQueryOkResponse | str = (
        client.get_datastore_query_dataset_id_index(
            dataset_id="0ba7-2cb0",
            index=0,
        )
    )
    assert isinstance(response, JsonOrCsvQueryOkResponse)
    sob.model.validate(response)
    response = client.get_datastore_query_dataset_id_index(
        dataset_id="0ba7-2cb0",
        index=0,
        format_="csv",
    )
    assert isinstance(response, str)


def test_client_get_datastore_query_dataset_id_index_download(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/query/{datasetId}/{index}/download
    """
    response: JsonOrCsvQueryOkResponse | str = (
        client.get_datastore_query_dataset_id_index_download(
            dataset_id="0ba7-2cb0",
            index=0,
            format_="csv",
        )
    )
    assert isinstance(response, str)


def test_client_post_datastore_query_dataset_id_index(
    client: Client,
) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/datastore/query/{datasetId}/{index}
    """
    response: JsonOrCsvQueryOkResponse | str = (
        client.post_datastore_query_dataset_id_index(
            DatastoreResourceQuery(),
            dataset_id="0ba7-2cb0",
            index=0,
        )
    )
    assert isinstance(response, JsonOrCsvQueryOkResponse)
    sob.model.validate(response)
    response = client.post_datastore_query_dataset_id_index(
        DatastoreResourceQuery(format_="csv"),
        dataset_id="0ba7-2cb0",
        index=0,
    )
    assert isinstance(response, str)


def test_get_datastore_sql(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/datastore/sql
    """
    response: DatastoreSqlGetResponse = client.get_datastore_sql(
        "[SELECT * FROM 1ee2fea0-00a3-58f4-8717-89b3cd62e442][LIMIT 2]",
        show_db_columns=True,
    )
    sob.model.validate(response)


def test_get_harvest_plans(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/harvest/plans
    """
    try:
        client.get_harvest_plans()
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to retrieve all harvest plans should "
            "result in a 401 error"
        )
        raise RuntimeError(message)


def test_post_harvest_plans(client: Client) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/harvest/plans
    """
    try:
        client.post_harvest_plans(
            HarvestPlan(
                identifier="h1",
                extract=HarvestPlanExtract(
                    type_="\\Drupal\\harvest\\ETL\\Extract\\DataJson",
                    uri=(
                        "https://dkan-default-content-files.s3.amazonaws.com"
                        "/data.json"
                    ),
                ),
                load=HarvestPlanLoad(type_="\\Drupal\\harvest\\Load\\Dataset"),
            )
        )
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to create a harvest plan should "
            "result in a 401 error"
        )
        raise RuntimeError(message)


def test_get_harvest_plans_plan_id(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/harvest/plans/{planId}
    """
    try:
        client.get_harvest_plans_plan_id(plan_id="h1")
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to get a harvest plan unauthenticated "
            "should result in a 401 error"
        )
        raise RuntimeError(message)


def test_get_harvest_runs(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/harvest/runs
    """
    try:
        client.get_harvest_runs(plan="p1")
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to get harvest runs unauthenticated "
            "should result in a 401 error"
        )
        raise RuntimeError(message)


def test_get_harvest_runs_run_id(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/harvest/runs/{runId}
    """
    try:
        client.get_harvest_runs_run_id(run_id="r1")
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to get a harvest run unauthenticated "
            "should result in a 401 error"
        )
        raise RuntimeError(message)


def test_post_harvest_runs(client: Client) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/harvest/runs
    """
    try:
        client.post_harvest_runs(
            HarvestRunsPostRequest(
                plan_id="p1",
            ),
        )
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        message: str = (
            "Attempting to post a harvest run unauthenticated "
            "should result in a 401 error"
        )
        raise RuntimeError(message)


def test_get_metastore_schemas(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas
    """
    response: sob.abc.Dictionary = client.get_metastore_schemas()
    sob.model.validate(response)


def test_get_metastore_schemas_schema_id(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas
    """
    key: str
    value: dict
    for key, value in islice(
        client.get_metastore_schemas().items(),
        # Only check the first 3 schemas
        3,
    ):
        # A lookup by schema ID should return the same object as was found
        # in the schema dictionary
        assert client.get_metastore_schemas_schema_id(key) == value


def test_get_metastore_schemas_schema_id_items(client: Client) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/{schemaId}/items
    """
    key: str
    for key in islice(
        client.get_metastore_schemas().keys(),
        # Only check the first 3 schemas
        3,
    ):
        items: MetastoreSchemasSchemaIdItemsGetResponse = (
            client.get_metastore_schemas_schema_id_items(
                key, show_reference_ids=True
            )
        )
        sob.model.validate(items)


def test_get_metastore_schemas_schema_id_items_identifier_revisions(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/{schema_id}/items/{identifier}/revisions
    """
    key: str
    for key in islice(
        client.get_metastore_schemas().keys(),
        # Only check the first 3 schemas
        3,
    ):
        items: MetastoreSchemasSchemaIdItemsGetResponse = (
            client.get_metastore_schemas_schema_id_items(
                key, show_reference_ids=True
            )
        )
        item: dict
        for item in islice(items, 3):
            try:
                (
                    client
                ).get_metastore_schemas_schema_id_items_identifier_revisions(
                    schema_id=key,
                    identifier=item["identifier"],
                )
            except HTTPError as error:
                if error.code != 401:
                    raise
            else:
                message: str = (
                    "Attempting to get item revisions unauthenticated "
                    "should result in a 401 error"
                )
                raise RuntimeError(message)


def test_post_metastore_schemas_schema_id_items_identifier_revisions(
    client: Client,
) -> None:
    """
    Test a POST request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/{schema_id}/items/{identifier}/revisions
    """
    key: str
    for key in islice(
        client.get_metastore_schemas().keys(),
        # Only check the first 3 schemas
        3,
    ):
        items: MetastoreSchemasSchemaIdItemsGetResponse = (
            client.get_metastore_schemas_schema_id_items(
                key, show_reference_ids=True
            )
        )
        item: dict
        for item in islice(items, 3):
            try:
                (
                    client
                ).post_metastore_schemas_schema_id_items_identifier_revisions(
                    MetastoreSchemaRevisionPostRequest(
                        message="Test revision",
                        state="draft",
                    ),
                    schema_id=key,
                    identifier=item["identifier"],
                )
            except HTTPError as error:
                if error.code != 401:
                    raise
            else:
                message: str = (
                    "Attempting to get item revisions unauthenticated "
                    "should result in a 401 error"
                )
                raise RuntimeError(message)


def test_get_metastore_schemas_schema_id_items_identifier_revisions_revision_id(  # noqa: E501
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/metastore/schemas/{schema_id}/items/{identifier}/revisions/{revision_id}
    """
    # Count items to make sure the test was not empty
    items_count: int = 0
    key: str
    for key in islice(
        client.get_metastore_schemas().keys(),
        # Only check the first 3 schemas
        3,
    ):
        items: MetastoreSchemasSchemaIdItemsGetResponse = (
            client.get_metastore_schemas_schema_id_items(
                key, show_reference_ids=True
            )
        )
        items_count += len(items)
        item: dict
        for item in islice(items, 3):
            try:
                revision: MetastoreRevision
                for revision in (
                    client
                ).get_metastore_schemas_schema_id_items_identifier_revisions(
                    schema_id=key,
                    identifier=item["identifier"],
                ):
                    (
                        client
                    ).get_metastore_schemas_schema_id_items_identifier_revisions_revision_id(
                        schema_id=key,
                        identifier=item["identifier"],
                        revision_id=revision.identifier or "",
                    )
            except HTTPError as error:
                if error.code != 401:
                    raise
            else:
                message: str = (
                    "Attempting to get a specific item revision "
                    "unauthenticated should result in a 401 error"
                )
                raise RuntimeError(message)
    assert items_count


def test_client_get_search(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/search
    """
    # Test a search that should return 0 results
    response: SearchGetResponse = client.get_search(
        theme="Supplier directory",
        keyword="Unknown Supplier/Provider Specialty",
        page=1,
        page_size=20,
    )
    sob.model.validate(response)
    assert response.total == "0"
    # Test a search that should return at least 1 result
    response = client.get_search(
        theme="Supplier directory",
        page=1,
        page_size=20,
    )
    assert int(response.total or 0) > 0
    sob.model.validate(response)


def test_client_get_search_facets(
    client: Client,
) -> None:
    """
    Test a GET request to
    https://data.cms.gov/provider-data/api/1/search/facets
    """
    response: SearchFacetsGetResponse = client.get_search_facets()
    sob.model.validate(response)


if __name__ == "__main__":
    pytest.main(["tests/test_integration.py"])
