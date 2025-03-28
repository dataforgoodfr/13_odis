from common.utils.interfaces.data_handler import MetadataInfo

from .stubs.data_handler import StubMetadataInfo


def test_metadata_info():
    # To be improved

    stub_metadata = StubMetadataInfo.from_dict()
    print(stub_metadata.model_dump(mode="json"))

    assert type(stub_metadata) is MetadataInfo
