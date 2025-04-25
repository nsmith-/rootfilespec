from rootfilespec.bootstrap.TObject import StreamHeader
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.structutil import ReadBuffer, ROOTSerializable


def read_streamed_item(buffer: ReadBuffer) -> tuple[ROOTSerializable, ReadBuffer]:
    # Read ahead the stream header to determine the type of the object
    itemheader, _ = StreamHeader.read(buffer)
    if itemheader.fByteCount == 0 and itemheader.fClassRef is not None:
        # TODO: implement dereferencing of fClassRef
        item, buffer = StreamHeader.read(buffer)
        return item, buffer
    if not itemheader.fClassName:
        msg = f"StreamHeader has no class name: {itemheader}"
        raise ValueError(msg)
    clsname = normalize(itemheader.fClassName)
    if clsname not in DICTIONARY:
        if clsname == "TLeafI":
            msg = (
                "TLeafI not declared in StreamerInfo perhaps? e.g. uproot-issue413.root\n"
                "(84 other test files have it, e.g. uproot-issue121.root)"
            )
            # https://github.com/scikit-hep/uproot3/issues/413
            # Likely groot-v0.21.0 (Go ROOT file implementation) did not write the streamers for TLeaf
            raise NotImplementedError(msg)
        msg = f"Unknown class name: {itemheader.fClassName}"
        msg += f"\nStreamHeader: {itemheader}"
        raise ValueError(msg)
    # Now actually read the object
    dyntype = DICTIONARY[clsname]
    item_end = itemheader.fByteCount + 4
    buffer, remaining = buffer[:item_end], buffer[item_end:]
    item, buffer = dyntype.read(buffer)
    if buffer:
        msg = f"Expected buffer to be empty after reading {dyntype}, but got\n{buffer}"
        raise ValueError(msg)
    assert buffer.local_refs == remaining.local_refs
    return item, remaining
