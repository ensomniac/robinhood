"""Small deterministic concurrency primitives for historical collection.

Historical work is I/O-bound, but its inputs are ranked and its failure order is
part of the evidence contract.  This module overlaps only a bounded batch at a
time and yields outcomes in input order.  Completion order therefore never
changes which candidate is accepted, which failure is reported first, or which
date is built.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Generic, Iterable, Iterator, TypeVar


ItemT = TypeVar("ItemT")
ValueT = TypeVar("ValueT")


@dataclass(frozen=True)
class OrderedTaskResult(Generic[ItemT, ValueT]):
    """One captured task outcome, yielded in the original input order."""

    item: ItemT
    value: ValueT | None = None
    error: Exception | None = None

    def unwrap(self) -> ValueT:
        if self.error is not None:
            raise self.error
        return self.value  # type: ignore[return-value]


def _captured_call(
    function: Callable[[ItemT], ValueT], item: ItemT
) -> OrderedTaskResult[ItemT, ValueT]:
    try:
        return OrderedTaskResult(item=item, value=function(item))
    except Exception as exc:
        return OrderedTaskResult(item=item, error=exc)


def ordered_bounded_results(
    items: Iterable[ItemT],
    function: Callable[[ItemT], ValueT],
    *,
    max_workers: int,
) -> Iterator[OrderedTaskResult[ItemT, ValueT]]:
    """Run bounded I/O concurrently and expose results deterministically.

    At most ``max_workers`` items are submitted speculatively.  The next batch
    is not submitted until the caller consumes the current batch, so breaking
    on a provider-wide or permanent fidelity failure cannot fan that failure out
    across an entire date or manifest.
    """
    if isinstance(max_workers, bool) or max_workers < 1:
        raise ValueError("max_workers must be a positive integer")
    iterator = iter(items)
    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="historical-collector",
    ) as executor:
        while True:
            batch: list[tuple[ItemT, Future[OrderedTaskResult[ItemT, ValueT]]]] = []
            for _ in range(max_workers):
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                batch.append((item, executor.submit(_captured_call, function, item)))
            if not batch:
                return
            for _, future in batch:
                yield future.result()
