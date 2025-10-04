"""
common.utils

Utility helper functions.
"""
def chunked(start: int, end: int, size: int):
    """
    Yield (start, end) subranges of given size.
    """
    cur = start
    while cur <= end:
        sub_end = min(cur + size - 1, end)
        yield (cur, sub_end)
        cur = sub_end + 1
