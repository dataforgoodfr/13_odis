import datetime

from common.data_source_model import DomainModel
from common.utils.interfaces.data_handler import (
    IDataHandler,
    MetadataInfo,
    OperationType,
    PageLog,
    ArtifactLog,
    StorageInfo,
)


class StubDataHandler(IDataHandler):
    """used to test the JsonExtractor class"""

    is_handled: bool = False

    def file_dump(self, *args, **kwargs) -> StorageInfo:

        self.is_handled = True
        return StorageInfo(
            location="test", format="test", file_name="test", encoding="test"
        )

    def dump_metadata(
        self,
        model: DomainModel,
        operation: OperationType,
        start_time: datetime = None,
        last_processed_page: int = None,
        complete: bool = None,
        errors: int = None,
        pages: list[PageLog] = None,
        artifacts: list[ArtifactLog] = None
    ):

        self.is_handled = True

        return MetadataInfo(
            **{
                "domain": model.domain_name,
                "source": model.name,
                "operation": operation,
                "last_run_time": start_time.isoformat(),
                "last_processed_page": last_processed_page,
                "complete": complete,
                "errors": errors,
                "model": model,
                "pages": pages,
                "artifacts": artifacts,
            }
        )


class StubMetadataInfo(MetadataInfo):

    @classmethod
    def from_dict(cls):

        stub_model = {
            "API": "INSEE.Melodi",
            "description": "nombre de maison",
            "type": "MelodiExtractor",
            "endpoint": "/data/DS_RP_LOGEMENT_PRINC",
            "format": "json",
            "extract_params": {
                "maxResult": 10000,
                "TIME_PERIOD": 2021,
                "GEO": ["COM", "DEP", "REG"],
                "RP_MEASURE": "DWELLINGS",
                "L_STAY": "_T",
                "TOH": "_T",
                "CARS": "_T",
                "NOR": "_T",
                "TSH": "_T",
                "BUILD_END": "_T",
                "OCS": "_T",
                "TDW": 1,
            },
            "response_map": {
                "data": "observations",
                "next": "paging.next",
                "is_last": "paging.isLast",
            },
        }

        nb_pages = 4
        stub_pages = []
        for i in range(1, nb_pages + 1):
            stub_pages.append(
                {
                    "page": i,
                    "storage_info": {
                        "location": "data/imports",
                        "format": "json",
                        "file_name": f"logement.logements_maison_et_residences_principales_{i}.json",
                        "encoding": "utf-8",
                    },
                    "is_last": False,
                    "success": True,
                }
            )

        stub_pages[-1]["is_last"] = True

        stub_artifacts = []
        stub_artifacts.append(
            {
                    "name": "logements_sociaux_epci",
                    "storage_info": {
                        "location": "data/imports",
                        "format": "xlsx",
                        "file_name": "logement.logements_sociaux_epci.json",
                        "encoding": "utf-8",
                    },
                    "load_to_bronze": True,
                    "success": True,
                }
        )

        stub_metadata = {
            "domain": "logement",
            "source": "logements_maisons",
            "operation": "extract",
            "last_run_time": datetime.datetime.now(
                tz=datetime.timezone.utc
            ).isoformat(),
            "last_processed_page": nb_pages,
            "extracted_pages": nb_pages,
            "loaded_pages": 0,
            "successfully_completed": True,
            "model": stub_model,
            "pages": stub_pages,
            "artifacts": stub_artifacts,
            "errors": 0,
            "complete": True,
        }

        return MetadataInfo(**stub_metadata)
