import logging
from typing import Callable, Any, List, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed

logger = logging.getLogger("gis_tool")


def parallel_process(
    func: Callable,
    items: Sequence,
    max_workers: int = None,
) -> List[Any]:
    """
    Run a function on a list of items in parallel using ProcessPoolExecutor.

    Args:
        func: Function to run on each item.
        items: Iterable of input items.
        max_workers: Max number of worker processes; defaults to number of processors.

    Returns:
        List of results in the original order of input items.
    """
    logger.info(f"parallel_process called with {len(items)} items.")
    results = [None] * len(items)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(func, item): idx for idx, item in enumerate(items)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
                logger.debug(f"parallel_process item {idx} completed.")
            except Exception as e:
                logger.error(f"Error in parallel_process for item {idx}: {e}")
                results[idx] = None
    return results
