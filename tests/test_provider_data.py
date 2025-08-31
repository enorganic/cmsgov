from __future__ import annotations

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
    JsonOrCsvQueryOkResponse,
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
        except HTTPError:
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
    except HTTPError:
        pass
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
    except HTTPError:
        pass
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
        format="csv", **kwargs
    )
    assert isinstance(response, str)
    response = client.get_datastore_query(format="json", **kwargs)
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
        client.get_datastore_query_download(format="csv", **kwargs)
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
        format="csv",
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
            format="csv",
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
        format="csv",
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


if __name__ == "__main__":
    pytest.main(["tests/test_integration.py"])
