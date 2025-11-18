# src/pipeline/extract_service.py

import asyncio
import time
from typing import List
from common.data_source_model import DataSourceModel, DomainModel
from common.utils.factory.extractor_factory import create_extractor
from common.utils.file_handler import FileHandler
from common.utils.http.async_client import AsyncHttpClient
from common.utils.logging_odis import logger


async def run_extraction(
    config_model: DataSourceModel,
    data_sources: List[DomainModel],
    max_concurrent_requests: int,
):
    """
    Pure business logic used by both CLI and Prefect.
    Extracts all given data sources asynchronously.
    """
    start_time = time.time()
    http_client = AsyncHttpClient(max_connections=max_concurrent_requests)

    tasks = []
    for ds in data_sources:
        logger.info(f"[extract] preparing extractor for {ds.name}")
        extractor = create_extractor(
            config_model, ds, http_client=http_client, handler=FileHandler()
        )
        tasks.append(extractor.execute())

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Detect errors
    errors = [res for res in results if isinstance(res, Exception)]
    if errors:
        for err in errors:
            logger.error(f"[extract] error: {err}")
        raise RuntimeError(f"{len(errors)} extraction errors occurred")

    elapsed = time.time() - start_time
    logger.info(f"[extract] completed in {elapsed:.2f}s for {len(data_sources)} sources")

    return results
