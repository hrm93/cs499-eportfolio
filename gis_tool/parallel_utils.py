"""
parallel_utils.py

Provides functionality to execute functions in parallel using ProcessPoolExecutor.
Used to accelerate processing of large datasets, particularly in geospatial workflows.

Author: Hannah Rose Morgenstein
Date: 2025-06-18
"""

import logging
from typing import Callable, Any, List, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configure module-level logger
logger = logging.getLogger("gis_tool.parallel_utils")


def parallel_process(
    func: Callable,
    items: Sequence,
    max_workers: int = None,
) -> List[Any]:
    """
    Run a function on a list of items in parallel using ProcessPoolExecutor.

    Args:
        func (Callable): Function to apply to each item in the input sequence.
        items (Sequence): Sequence of input items to be processed.
        max_workers (int, optional): Maximum number of worker processes. If None,
                                     defaults to the number of processors on the machine.

    Returns:
        List[Any]: Results of processing each item, in the original order.
    """
    logger.info(f"parallel_process called with {len(items)} items.")

    # Pre-allocate list for results
    results = [None] * len(items)

    # Submit tasks to the process pool
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(func, item): idx for idx, item in enumerate(items)}

        # Collect results as tasks complete
        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
                logger.debug(f"parallel_process item {idx} completed.")
            except Exception as e:
                logger.error(f"Error in parallel_process for item {idx}: {e}")
                results[idx] = None

    return results
