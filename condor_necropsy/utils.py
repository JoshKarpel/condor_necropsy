def table(headers, rows, fill="", header_fmt=None, row_fmt=None, alignment=None) -> str:
    """
    Return a string containing a simple table created from headers and rows of entries.

    Parameters
    ----------
    headers
        The column headers for the table.
    rows
        The entries for each row, for each column.
        Should be an iterable of iterables or mappings, with the outer level containing the rows,
        and each inner iterable containing the entries for each column.
        An iterable-type row is printed in order.
        A mapping-type row uses the headers as keys to align the stdout and can have missing values,
        which are filled using the ```fill`` value.
    fill
        The string to print in place of a missing value in a mapping-type row.
    header_fmt
        A function to be called on the header string.
        The return value is what will go in the output.
    row_fmt
        A function to be called on each row string.
        The return value is what will go in the output.
    alignment
        If ``True``, the first column will be left-aligned instead of centered.

    Returns
    -------
    table :
        A string containing the table.
    """
    if header_fmt is None:
        header_fmt = lambda _: _
    if row_fmt is None:
        row_fmt = lambda _: _
    if alignment is None:
        alignment = {}

    headers = tuple(headers)
    lengths = [len(h) for h in headers]

    align_methods = [alignment.get(h, "center") for h in headers]

    processed_rows = []
    for row in rows:
        if isinstance(row, dict):
            processed_rows.append([str(row.get(key, fill)) for key in headers])
        else:
            processed_rows.append([str(entry) for entry in row])

    for row in processed_rows:
        lengths = [max(curr, len(entry)) for curr, entry in zip(lengths, row)]

    header = header_fmt(
        "  ".join(
            getattr(h, a)(l) for h, l, a in zip(headers, lengths, align_methods)
        ).rstrip()
    )

    lines = (
        row_fmt(
            "  ".join(getattr(f, a)(l) for f, l, a in zip(row, lengths, align_methods))
        )
        for row in processed_rows
    )

    output = "\n".join((header, *lines))

    return rstr(output)


class rstr(str):
    """
    Identical to a normal Python string, except that it's ``__repr__``
    is its ``__str__``, to make it work nicer in notebooks.
    """

    def __repr__(self):
        return self.__str__()


def num_bytes_to_str(num_bytes):
    """Return a number of bytes as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return "{:.1f} {}".format(num_bytes, unit)
        num_bytes /= 1024
    return "{:.1f} TB".format(num_bytes)
