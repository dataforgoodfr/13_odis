from common.utils.interfaces.data_handler import MetadataInfo

from .stubs.data_handler import StubMetadataInfo


def test_metadata_info():
    # To be improved

    stub_metadata = StubMetadataInfo.from_dict()

    assert type(stub_metadata) is MetadataInfo
