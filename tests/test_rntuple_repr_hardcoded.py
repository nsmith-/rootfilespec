from pathlib import Path

import pytest
from skhep_testdata import data_path  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOT3a3aRNTuple, ROOTFile
from rootfilespec.buffer import ReadBuffer

TESTABLE_FILES = [
    "rntviewer-testfile-uncomp-single-rntuple-v1-0-0-0.root",
    "rntviewer-testfile-multiple-rntuples-v1-0-0-0.root",
]

# Hardcoded dictionary representation of objects in rntuple test files
# TODO: Add hardcoded representation of Header Envelope
# TODO: Add hardcoded representation of cluster_column_page_lists
#       requires implementing deserializing/decompressing RPages
DICTIONARY_REPR_HARDCODED = {
    "rntviewer-testfile-uncomp-single-rntuple-v1-0-0-0.root": {
        "file": r"ROOTFile(magic=b'root', fVersion=VersionInfo(major=6, minor=35, cycle=1, large=False), header=ROOTFile_header_v622_small(fBEGIN=100, fEND=2514, fSeekFree=2463, fNbytesFree=51, nfree=1, fNbytesName=60, fUnits=4, fCompress=0, fSeekInfo=2066, fNbytesInfo=397, fUUID=TUUID(fVersion=0, fUUID=UUID('00000000-0000-0000-0000-000000000000'))), padding=b'\x00\x00\x00\x00\x00\\\\\xf0\x9f\x90\xa3//\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')",
        "tfile": r"TFile(fName=TString(fString=b'RNTuple.root'), fTitle=TString(fString=b''), rootdir=TDirectory(header=TDirectory_header_v622(fVersion=5, fDatimeC=1994052632, fDatimeM=1994052632, fNbytesKeys=99, fNbytesName=60), fSeekDir=100, fSeekParent=0, fSeekKeys=1967, fUUID=TUUID(fVersion=1, fUUID=UUID('00000000-0000-0000-0000-000000000000'))))",
        "keylist": r"TKeyList(fKeys=[TKey(header=TKey_header(fNbytes=132, fVersion=4, fObjlen=78, fDatime=1994052632, fKeylen=54, fCycle=1), fSeekKey=1835, fSeekPdir=100, fClassName=TString(fString=b'ROOT::RNTuple'), fName=TString(fString=b'Contributors'), fTitle=TString(fString=b''))], padding=b'')",
        "streamerinfo": r"TList(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b''), fSize=1, items=[TStreamerInfo(fVersion=1, fUniqueID=0, fBits=TObjFlag(TObjFlag.kNotSure), pidf=None, fName=TString(fString=b'ROOT::RNTuple'), fTitle=TString(fString=b''), fCheckSum=686174956, fClassVersion=2, fObjects=TObjArray(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b''), fSize=11, fLowerBound=0, objects=(TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionEpoch'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionMajor'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionMinor'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionPatch'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fSeekHeader'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fNBytesHeader'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fLenHeader'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fSeekFooter'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fNBytesFooter'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fLenFooter'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fMaxKeySize'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')))))])",
        "RNTuple:Contributors": {
            "anchor": r"ROOT3a3aRNTuple(fVersionEpoch=1, fVersionMajor=0, fVersionMinor=0, fVersionPatch=0, fSeekHeader=254, fNBytesHeader=332, fLenHeader=332, fSeekFooter=1687, fNBytesFooter=148, fLenFooter=148, fMaxKeySize=1073741824)",
            "footer": r"FooterEnvelope(typeID=2, length=148, checksum=9038192899957947137, featureFlags=RFeatureFlags(flags=0), headerChecksum=9346497350689737328, schemaExtension=SchemaExtension(fSize=56), clusterGroups=ListFrame(fSize=60, items=[ClusterGroup(fSize=48, fMinEntryNumber=0, fEntrySpan=22, fNClusters=1, pagelistLink=REnvelopeLink(length=244, locator=StandardLocator(size=244, offset=1409)))]))",
            "page_location_lists": r"[PageListEnvelope(typeID=3, length=244, checksum=12340257838343085244, headerChecksum=9346497350689737328, clusterSummaries=ListFrame(fSize=36, items=[ClusterSummary(fSize=24, fFirstEntryNumber=0, fNEntriesAndFeatureFlag=22)]), pageLocations=ClusterLocations(fSize=184, items=[ColumnLocations(fSize=172, items=[PageLocations(fSize=40, items=[RPageDescription(fNElements=-22, locator=StandardLocator(size=176, offset=620))], elementoffset=0, compressionsettings=0), PageLocations(fSize=40, items=[RPageDescription(fNElements=-178, locator=StandardLocator(size=178, offset=804))], elementoffset=0, compressionsettings=0), PageLocations(fSize=40, items=[RPageDescription(fNElements=-22, locator=StandardLocator(size=176, offset=990))], elementoffset=0, compressionsettings=0), PageLocations(fSize=40, items=[RPageDescription(fNElements=-193, locator=StandardLocator(size=193, offset=1174))], elementoffset=0, compressionsettings=0)])]))]",
            # "cluster_column_page_lists":
        },
    },
    "rntviewer-testfile-multiple-rntuples-v1-0-0-0.root": {
        "file": r"ROOTFile(magic=b'root', fVersion=VersionInfo(major=6, minor=35, cycle=1, large=False), header=ROOTFile_header_v622_small(fBEGIN=100, fEND=2382, fSeekFree=936, fNbytesFree=74, nfree=3, fNbytesName=64, fUnits=4, fCompress=505, fSeekInfo=1038, fNbytesInfo=405, fUUID=TUUID(fVersion=1, fUUID=UUID('5c45ad9e-da41-11ef-9a5a-6432a8c0beef'))), padding=b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')",
        "tfile": r"TFile(fName=TString(fString=b'multi.root'), fTitle=TString(fString=b''), rootdir=TDirectory(header=TDirectory_header_v622(fVersion=5, fDatimeC=2020654388, fDatimeM=2020654388, fNbytesKeys=142, fNbytesName=64), fSeekDir=100, fSeekParent=0, fSeekKeys=2240, fUUID=TUUID(fVersion=1, fUUID=UUID('5c45ad9e-da41-11ef-9a5a-6432a8c0beef'))))",
        "keylist": r"TKeyList(fKeys=[TKey(header=TKey_header(fNbytes=129, fVersion=1004, fObjlen=78, fDatime=2020654388, fKeylen=51, fCycle=1), fSeekKey=807, fSeekPdir=100, fClassName=TString(fString=b'ROOT::RNTuple'), fName=TString(fString=b'A'), fTitle=TString(fString=b'')), TKey(header=TKey_header(fNbytes=121, fVersion=4, fObjlen=78, fDatime=2020654388, fKeylen=43, fCycle=1), fSeekKey=2119, fSeekPdir=100, fClassName=TString(fString=b'ROOT::RNTuple'), fName=TString(fString=b'B'), fTitle=TString(fString=b''))], padding=b'')",
        "streamerinfo": r"TList(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b''), fSize=1, items=[TStreamerInfo(fVersion=1, fUniqueID=0, fBits=TObjFlag(TObjFlag.kNotSure), pidf=None, fName=TString(fString=b'ROOT::RNTuple'), fTitle=TString(fString=b''), fCheckSum=686174956, fClassVersion=2, fObjects=TObjArray(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b''), fSize=11, fLowerBound=0, objects=(TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionEpoch'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionMajor'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionMinor'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fVersionPatch'), fTitle=TString(fString=b''), fType=ElementType.kUShort, fSize=2, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned short')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fSeekHeader'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fNBytesHeader'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fLenHeader'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fSeekFooter'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fNBytesFooter'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fLenFooter'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')), TStreamerBasicType(fVersion=1, fUniqueID=0, fBits=TObjFlag(), pidf=None, fName=TString(fString=b'fMaxKeySize'), fTitle=TString(fString=b''), fType=ElementType.kULong, fSize=8, fArrayLength=0, fArrayDim=0, fMaxIndex=ArrayDim(dim0=0, dim1=0, dim2=0, dim3=0, dim4=0), fTypeName=TString(fString=b'unsigned long')))))])",
        "RNTuple:A": {
            "anchor": r"ROOT3a3aRNTuple(fVersionEpoch=1, fVersionMajor=0, fVersionMinor=0, fVersionPatch=0, fSeekHeader=266, fNBytesHeader=101, fLenHeader=164, fSeekFooter=725, fNBytesFooter=82, fLenFooter=148, fMaxKeySize=1073741824)",
            "footer": r"FooterEnvelope(typeID=2, length=148, checksum=16904131729352343975, featureFlags=RFeatureFlags(flags=0), headerChecksum=1772847515747675522, schemaExtension=SchemaExtension(fSize=56), clusterGroups=ListFrame(fSize=60, items=[ClusterGroup(fSize=48, fMinEntryNumber=0, fEntrySpan=100, fNClusters=1, pagelistLink=REnvelopeLink(length=124, locator=StandardLocator(size=86, offset=597)))]))",
            "page_location_lists": r"[PageListEnvelope(typeID=3, length=124, checksum=748677678342101309, headerChecksum=1772847515747675522, clusterSummaries=ListFrame(fSize=36, items=[ClusterSummary(fSize=24, fFirstEntryNumber=0, fNEntriesAndFeatureFlag=100)]), pageLocations=ClusterLocations(fSize=64, items=[ColumnLocations(fSize=52, items=[PageLocations(fSize=40, items=[RPageDescription(fNElements=-100, locator=StandardLocator(size=138, offset=409))], elementoffset=0, compressionsettings=505)])]))]",
            # "cluster_column_page_lists":
        },
        "RNTuple:B": {
            "anchor": r"ROOT3a3aRNTuple(fVersionEpoch=1, fVersionMajor=0, fVersionMinor=0, fVersionPatch=0, fSeekHeader=1542, fNBytesHeader=111, fLenHeader=171, fSeekFooter=2037, fNBytesFooter=82, fLenFooter=148, fMaxKeySize=1073741824)",
            "footer": r"FooterEnvelope(typeID=2, length=148, checksum=17038928962946065552, featureFlags=RFeatureFlags(flags=0), headerChecksum=14068653553654343426, schemaExtension=SchemaExtension(fSize=56), clusterGroups=ListFrame(fSize=60, items=[ClusterGroup(fSize=48, fMinEntryNumber=0, fEntrySpan=100, fNClusters=1, pagelistLink=REnvelopeLink(length=124, locator=StandardLocator(size=86, offset=1909)))]))",
            "page_location_lists": r"[PageListEnvelope(typeID=3, length=124, checksum=674435399773528910, headerChecksum=14068653553654343426, clusterSummaries=ListFrame(fSize=36, items=[ClusterSummary(fSize=24, fFirstEntryNumber=0, fNEntriesAndFeatureFlag=100)]), pageLocations=ClusterLocations(fSize=64, items=[ColumnLocations(fSize=52, items=[PageLocations(fSize=40, items=[RPageDescription(fNElements=-100, locator=StandardLocator(size=164, offset=1695))], elementoffset=0, compressionsettings=505)])]))]",
            # "cluster_column_page_lists":
        },
    },
}


@pytest.mark.parametrize("filename", TESTABLE_FILES)
def test_rntuple_repr_hardcoded(filename: str):
    initial_read_size = 512
    path = Path(data_path(filename))
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            filehandle.seek(seek)
            return ReadBuffer(memoryview(filehandle.read(size)), seek, 0)

        buffer = fetch_data(0, initial_read_size)
        file, _ = ROOTFile.read(buffer)
        assert repr(file) == DICTIONARY_REPR_HARDCODED[filename]["file"]

        # Read root directory object, which should be contained in the initial buffer
        def fetch_cached(seek: int, size: int):
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            msg = "Didn't find data in initial read buffer"
            raise ValueError(msg)

        # Get the TFile object from the ROOT file
        tfile = file.get_TFile(fetch_cached)
        assert repr(tfile) == DICTIONARY_REPR_HARDCODED[filename]["tfile"]

        # Get TKeyList (List of all TKeys in the TDirectory)
        keylist = tfile.get_KeyList(fetch_data)
        assert repr(keylist) == DICTIONARY_REPR_HARDCODED[filename]["keylist"]

        # Get TStreamerInfo (List of classes used in the file)
        streamerinfo = file.get_StreamerInfo(fetch_data)
        assert repr(streamerinfo) == DICTIONARY_REPR_HARDCODED[filename]["streamerinfo"]

        for name, tkey in keylist.items():
            # Get RNTuple Anchor Object
            anchor = tkey.read_object(fetch_data, ROOT3a3aRNTuple)
            assert (
                repr(anchor)
                == DICTIONARY_REPR_HARDCODED[filename][f"RNTuple:{name}"]["anchor"]
            )

            # Get the RNTuple Footer Envelope from the Anchor
            footer = anchor.get_footer(fetch_data)
            assert (
                repr(footer)
                == DICTIONARY_REPR_HARDCODED[filename][f"RNTuple:{name}"]["footer"]
            )

            # Get the RNTuple Page List Envelopes from the Footer Envelope
            page_location_lists = footer.get_pagelists(fetch_data)
            assert (
                repr(page_location_lists)
                == DICTIONARY_REPR_HARDCODED[filename][f"RNTuple:{name}"][
                    "page_location_lists"
                ]
            )
