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
        msg = f"Unknown class name: {itemheader.fClassName}"
        msg += f"\nStreamHeader: {itemheader}"
        raise ValueError(msg)
    # Now actually read the object
    item, buffer = DICTIONARY[clsname].read(buffer)
    return item, buffer
