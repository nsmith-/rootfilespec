from rootfilespec.rntuple.envelope import ENVELOPE_TYPE_MAP, REnvelope
from rootfilespec.serializable import serializable


@serializable
class HeaderEnvelope(REnvelope):
    """A class representing the RNTuple Header Envelope payload structure"""


ENVELOPE_TYPE_MAP[0x01] = "HeaderEnvelope"
