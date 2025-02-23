import random
import string

import pytest

from cognite.client.data_classes import DataSet, Transformation, TransformationDestination, TransformationUpdate
from cognite.client.data_classes.transformations._alphatypes import AlphaDataModelInstances
from cognite.client.data_classes.transformations.common import SequenceRows


@pytest.fixture
def new_datasets(cognite_client):
    ds_ext_id1 = "transformation-ds"
    ds_ext_id2 = "transformation-ds2"
    ds1 = cognite_client.data_sets.retrieve(external_id=ds_ext_id1)
    ds2 = cognite_client.data_sets.retrieve(external_id=ds_ext_id2)
    if not ds1:
        data_set1 = DataSet(name=ds_ext_id1, external_id=ds_ext_id1)
        ds1 = cognite_client.data_sets.create(data_set1)
    if not ds2:
        data_set2 = DataSet(name=ds_ext_id2, external_id=ds_ext_id2)
        ds2 = cognite_client.data_sets.create(data_set2)
    yield ds1, ds2


@pytest.fixture
def new_transformation(cognite_client, new_datasets):
    prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
    transform = Transformation(
        name="any",
        external_id=f"{prefix}-transformation",
        destination=TransformationDestination.assets(),
        data_set_id=new_datasets[0].id,
    )
    ts = cognite_client.transformations.create(transform)

    yield ts

    cognite_client.transformations.delete(id=ts.id)
    assert cognite_client.transformations.retrieve(ts.id) is None


other_transformation = new_transformation


class TestTransformationsAPI:
    def test_create_transformation_error(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform_without_name = Transformation(
            external_id=f"{prefix}-transformation", destination=TransformationDestination.assets()
        )
        try:
            ts = cognite_client.transformations.create(transform_without_name)
            failed = False
            cognite_client.transformations.delete(id=ts.id)
        except Exception as ex:
            failed = True
            str(ex)
        assert failed

    def test_create_asset_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any", external_id=f"{prefix}-transformation", destination=TransformationDestination.assets()
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_raw_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.raw("myDatabase", "myTable"),
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)
        assert ts.destination == TransformationDestination.raw("myDatabase", "myTable")

    def test_create_asset_hierarchy_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any", external_id=f"{prefix}-transformation", destination=TransformationDestination.asset_hierarchy()
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    def test_create_string_datapoints_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.string_datapoints(),
        )
        ts = cognite_client.transformations.create(transform)
        cognite_client.transformations.delete(id=ts.id)

    @pytest.mark.skip
    def test_create_alpha_dmi_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=AlphaDataModelInstances(
                model_external_id="testInstance",
                space_external_id="test-space",
                instance_space_external_id="test-space",
            ),
        )
        ts = cognite_client.transformations.create(transform)
        assert (
            ts.destination.type == "alpha_data_model_instances"
            and ts.destination.model_external_id == "testInstance"
            and ts.destination.space_external_id == "test-space"
            and ts.destination.instance_space_external_id == "test-space"
        )
        cognite_client.transformations.delete(id=ts.id)

    def test_create_sequence_rows_transformation(self, cognite_client):
        prefix = "".join(random.choice(string.ascii_letters) for i in range(6))
        transform = Transformation(
            name="any",
            external_id=f"{prefix}-transformation",
            destination=TransformationDestination.sequence_rows(external_id="testSequenceRows"),
        )
        ts = cognite_client.transformations.create(transform)
        assert ts.destination.type == "sequence_rows" and ts.destination.external_id == "testSequenceRows"
        cognite_client.transformations.delete(id=ts.id)

    def test_create(self, new_transformation):
        assert (
            new_transformation.name == "any"
            and new_transformation.destination == TransformationDestination.assets()
            and new_transformation.id is not None
        )

    def test_retrieve(self, cognite_client, new_transformation):
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            new_transformation.name == retrieved_transformation.name
            and new_transformation.destination == retrieved_transformation.destination
            and new_transformation.id == retrieved_transformation.id
        )

    def test_retrieve_multiple(self, cognite_client, new_transformation, other_transformation):
        retrieved_transformations = cognite_client.transformations.retrieve_multiple(
            ids=[new_transformation.id, other_transformation.id]
        )
        assert len(retrieved_transformations) == 2
        assert new_transformation.id in [
            transformation.id for transformation in retrieved_transformations
        ] and other_transformation.id in [transformation.id for transformation in retrieved_transformations]

    def test_update_full(self, cognite_client, new_transformation, new_datasets):
        expected_external_id = f"m__{new_transformation.external_id}"
        new_transformation.external_id = expected_external_id
        new_transformation.name = "new name"
        new_transformation.query = "SELECT * from _cdf.assets"
        new_transformation.destination = TransformationDestination.raw("myDatabase", "myTable")
        new_transformation.data_set_id = new_datasets[1].id
        updated_transformation = cognite_client.transformations.update(new_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.external_id == retrieved_transformation.external_id == expected_external_id
            and updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
            and updated_transformation.destination == TransformationDestination.raw("myDatabase", "myTable")
            and updated_transformation.data_set_id == new_datasets[1].id
        )

    def test_update_partial(self, cognite_client, new_transformation):
        expected_external_id = f"m__{new_transformation.external_id}"
        update_transformation = (
            TransformationUpdate(id=new_transformation.id)
            .external_id.set(expected_external_id)
            .name.set("new name")
            .query.set("SELECT * from _cdf.assets")
        )
        updated_transformation = cognite_client.transformations.update(update_transformation)
        retrieved_transformation = cognite_client.transformations.retrieve(new_transformation.id)
        assert (
            updated_transformation.external_id == retrieved_transformation.external_id == expected_external_id
            and updated_transformation.name == retrieved_transformation.name == "new name"
            and updated_transformation.query == retrieved_transformation.query == "SELECT * from _cdf.assets"
        )

    def test_list(self, cognite_client, new_transformation, new_datasets):
        # Filter by destination type
        retrieved_transformations = cognite_client.transformations.list(limit=None, destination_type="assets")
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

        # Filter by data set id
        retrieved_transformations = cognite_client.transformations.list(limit=None, data_set_ids=[new_datasets[0].id])
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

        # Filter by data set external id
        retrieved_transformations = cognite_client.transformations.list(
            limit=None, data_set_external_ids=[new_datasets[0].external_id]
        )
        assert new_transformation.id in [transformation.id for transformation in retrieved_transformations]

    def test_preview(self, cognite_client):
        query_result = cognite_client.transformations.preview(query="select 1 as id, 'asd' as name", limit=100)
        assert (
            query_result.schema is not None
            and query_result.results is not None
            and len(query_result.schema) == 2
            and len(query_result.results) == 1
            and query_result.results[0]["id"] == 1
            and query_result.results[0]["name"] == "asd"
        )

    def test_preview_to_string(self, cognite_client):
        query_result = cognite_client.transformations.preview(query="select 1 as id, 'asd' as name", limit=100)
        # just make sure it doesnt raise exceptions
        str(query_result)

    @pytest.mark.skip
    def test_update_dmi_alpha(self, cognite_client, new_transformation):
        new_transformation.destination = AlphaDataModelInstances("myTest", "test-space", "test-space")
        partial_update = TransformationUpdate(id=new_transformation.id).destination.set(
            AlphaDataModelInstances("myTest2", "test-space", "test-space")
        )
        updated_transformation = cognite_client.transformations.update(new_transformation)
        assert updated_transformation.destination == AlphaDataModelInstances("myTest", "test-space", "test-space")
        partial_updated = cognite_client.transformations.update(partial_update)
        assert partial_updated.destination == AlphaDataModelInstances("myTest2", "test-space", "test-space")

    def test_update_sequence_rows_update(self, cognite_client, new_transformation):
        new_transformation.destination = SequenceRows("myTest")
        updated_transformation = cognite_client.transformations.update(new_transformation)
        assert updated_transformation.destination == TransformationDestination.sequence_rows("myTest")

        partial_update = TransformationUpdate(id=new_transformation.id).destination.set(SequenceRows("myTest2"))
        partial_updated = cognite_client.transformations.update(partial_update)
        assert partial_updated.destination == TransformationDestination.sequence_rows("myTest2")
