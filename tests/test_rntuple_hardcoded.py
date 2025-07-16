from pathlib import Path

from skhep_testdata import data_path  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOT3a3aRNTuple, ROOTFile
from rootfilespec.bootstrap.strings import RString
from rootfilespec.buffer import ReadBuffer
from rootfilespec.rntuple.envelope import REnvelopeLink, RFeatureFlags
from rootfilespec.rntuple.footer import ClusterGroup, FooterEnvelope, SchemaExtension
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.pagelist import ClusterSummary, PageListEnvelope
from rootfilespec.rntuple.pagelocations import PageLocations, RPageDescription
from rootfilespec.rntuple.RFrame import ListFrame
from rootfilespec.rntuple.RLocator import StandardLocator
from rootfilespec.rntuple.schema import (
    ColumnDescription,
    ColumnType,
    FieldDescription,
)

# TODO: Add hardcoded representation of RPages once implemented
# TODO: Add hardcoded representation of RNTuple once implemented?
# TODO: Add test for a more complex RNTuple with complex schema and multiple clusters?


def test_read_contributors():
    filename = "rntviewer-testfile-uncomp-single-rntuple-v1-0-0-0.root"
    path = Path(data_path(filename))
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            filehandle.seek(seek)
            return ReadBuffer(memoryview(filehandle.read(size)), seek, 0)

        buffer = fetch_data(0, 512)
        file, _ = ROOTFile.read(buffer)
        tfile = file.get_TFile(fetch_data)
        keylist = tfile.get_KeyList(fetch_data)
        anchor = keylist["Contributors"].read_object(fetch_data, ROOT3a3aRNTuple)

        assert anchor == ROOT3a3aRNTuple(
            fVersionEpoch=1,
            fVersionMajor=0,
            fVersionMinor=0,
            fVersionPatch=0,
            fSeekHeader=254,
            fNBytesHeader=332,
            fLenHeader=332,
            fSeekFooter=1687,
            fNBytesFooter=148,
            fLenFooter=148,
            fMaxKeySize=1073741824,
        )

        header = anchor.get_header(fetch_data)
        assert header == HeaderEnvelope(
            typeID=1,
            length=332,
            checksum=9346497350689737328,
            featureFlags=RFeatureFlags(flags=0),
            fName=RString(fString=b"Contributors"),
            fDescription=RString(fString=b"The first ever RNTuple."),
            fLibrary=RString(fString=b"ROOT v6.35.001"),
            fieldDescriptions=ListFrame(
                fSize=131,
                items=[
                    FieldDescription(
                        fSize=60,
                        fFieldVersion=0,
                        fTypeVersion=0,
                        fParentFieldID=0,
                        fStructuralRole=0,
                        fFlags=0,
                        fFieldName=RString(fString=b"firstName"),
                        fTypeName=RString(fString=b"std::string"),
                        fTypeAlias=RString(fString=b""),
                        fFieldDescription=RString(fString=b""),
                        fArraySize=None,
                        fSourceFieldID=None,
                        fTypeChecksum=None,
                    ),
                    FieldDescription(
                        fSize=59,
                        fFieldVersion=0,
                        fTypeVersion=0,
                        fParentFieldID=1,
                        fStructuralRole=0,
                        fFlags=0,
                        fFieldName=RString(fString=b"lastName"),
                        fTypeName=RString(fString=b"std::string"),
                        fTypeAlias=RString(fString=b""),
                        fFieldDescription=RString(fString=b""),
                        fArraySize=None,
                        fSourceFieldID=None,
                        fTypeChecksum=None,
                    ),
                ],
            ),
            columnDescriptions=ListFrame(
                fSize=92,
                items=[
                    ColumnDescription(
                        fSize=20,
                        fColumnType=ColumnType.kIndex64,
                        fBitsOnStorage=64,
                        fFieldID=0,
                        fFlags=0,
                        fRepresentationIndex=0,
                        fFirstElementIndex=None,
                        fMinValue=None,
                        fMaxValue=None,
                    ),
                    ColumnDescription(
                        fSize=20,
                        fColumnType=ColumnType.kChar,
                        fBitsOnStorage=8,
                        fFieldID=0,
                        fFlags=0,
                        fRepresentationIndex=0,
                        fFirstElementIndex=None,
                        fMinValue=None,
                        fMaxValue=None,
                    ),
                    ColumnDescription(
                        fSize=20,
                        fColumnType=ColumnType.kIndex64,
                        fBitsOnStorage=64,
                        fFieldID=1,
                        fFlags=0,
                        fRepresentationIndex=0,
                        fFirstElementIndex=None,
                        fMinValue=None,
                        fMaxValue=None,
                    ),
                    ColumnDescription(
                        fSize=20,
                        fColumnType=ColumnType.kChar,
                        fBitsOnStorage=8,
                        fFieldID=1,
                        fFlags=0,
                        fRepresentationIndex=0,
                        fFirstElementIndex=None,
                        fMinValue=None,
                        fMaxValue=None,
                    ),
                ],
            ),
            aliasColumnDescriptions=ListFrame(fSize=12, items=[]),
            extraTypeInformations=ListFrame(fSize=12, items=[]),
        )

        footer = anchor.get_footer(fetch_data)
        assert footer == FooterEnvelope(
            typeID=2,
            length=148,
            checksum=9038192899957947137,
            featureFlags=RFeatureFlags(flags=0),
            headerChecksum=9346497350689737328,
            schemaExtension=SchemaExtension(
                fSize=56,
                fieldDescriptions=ListFrame(fSize=12, items=[]),
                columnDescriptions=ListFrame(fSize=12, items=[]),
                aliasColumnDescriptions=ListFrame(fSize=12, items=[]),
                extraTypeInformations=ListFrame(fSize=12, items=[]),
            ),
            clusterGroups=ListFrame(
                fSize=60,
                items=[
                    ClusterGroup(
                        fSize=48,
                        fMinEntryNumber=0,
                        fEntrySpan=22,
                        fNClusters=1,
                        pagelistLink=REnvelopeLink(
                            length=244, locator=StandardLocator(size=244, offset=1409)
                        ),
                    )
                ],
            ),
        )

        page_location_lists = footer.get_pagelists(fetch_data)
        assert page_location_lists == [
            PageListEnvelope(
                typeID=3,
                length=244,
                checksum=12340257838343085244,
                headerChecksum=9346497350689737328,
                clusterSummaries=ListFrame(
                    fSize=36,
                    items=[
                        ClusterSummary(
                            fSize=24, fFirstEntryNumber=0, fNEntriesAndFeatureFlag=22
                        )
                    ],
                ),
                pageLocations=ListFrame(
                    fSize=184,
                    items=[
                        ListFrame(
                            fSize=172,
                            items=[
                                PageLocations(
                                    fSize=40,
                                    items=[
                                        RPageDescription(
                                            fNElements=-22,
                                            locator=StandardLocator(
                                                size=176, offset=620
                                            ),
                                        )
                                    ],
                                    elementoffset=0,
                                    compressionsettings=0,
                                ),
                                PageLocations(
                                    fSize=40,
                                    items=[
                                        RPageDescription(
                                            fNElements=-178,
                                            locator=StandardLocator(
                                                size=178, offset=804
                                            ),
                                        )
                                    ],
                                    elementoffset=0,
                                    compressionsettings=0,
                                ),
                                PageLocations(
                                    fSize=40,
                                    items=[
                                        RPageDescription(
                                            fNElements=-22,
                                            locator=StandardLocator(
                                                size=176, offset=990
                                            ),
                                        )
                                    ],
                                    elementoffset=0,
                                    compressionsettings=0,
                                ),
                                PageLocations(
                                    fSize=40,
                                    items=[
                                        RPageDescription(
                                            fNElements=-193,
                                            locator=StandardLocator(
                                                size=193, offset=1174
                                            ),
                                        )
                                    ],
                                    elementoffset=0,
                                    compressionsettings=0,
                                ),
                            ],
                        )
                    ],
                ),
            )
        ]


def test_read_multiple_rntuples():
    filename = "rntviewer-testfile-multiple-rntuples-v1-0-0-0.root"
    path = Path(data_path(filename))
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            filehandle.seek(seek)
            return ReadBuffer(memoryview(filehandle.read(size)), seek, 0)

        buffer = fetch_data(0, 512)
        file, _ = ROOTFile.read(buffer)
        tfile = file.get_TFile(fetch_data)
        keylist = tfile.get_KeyList(fetch_data)

        rntuple_a = keylist["A"].read_object(fetch_data, ROOT3a3aRNTuple)
        assert rntuple_a == ROOT3a3aRNTuple(
            fVersionEpoch=1,
            fVersionMajor=0,
            fVersionMinor=0,
            fVersionPatch=0,
            fSeekHeader=266,
            fNBytesHeader=101,
            fLenHeader=164,
            fSeekFooter=725,
            fNBytesFooter=82,
            fLenFooter=148,
            fMaxKeySize=1073741824,
        )

        header_a = rntuple_a.get_header(fetch_data)
        assert header_a == HeaderEnvelope(
            typeID=1,
            length=164,
            checksum=1772847515747675522,
            featureFlags=RFeatureFlags(flags=0),
            fName=RString(fString=b"A"),
            fDescription=RString(fString=b""),
            fLibrary=RString(fString=b"ROOT v6.35.01"),
            fieldDescriptions=ListFrame(
                fSize=58,
                items=[
                    FieldDescription(
                        fSize=46,
                        fFieldVersion=0,
                        fTypeVersion=0,
                        fParentFieldID=0,
                        fStructuralRole=0,
                        fFlags=0,
                        fFieldName=RString(fString=b"f"),
                        fTypeName=RString(fString=b"float"),
                        fTypeAlias=RString(fString=b""),
                        fFieldDescription=RString(fString=b""),
                        fArraySize=None,
                        fSourceFieldID=None,
                        fTypeChecksum=None,
                    )
                ],
            ),
            columnDescriptions=ListFrame(
                fSize=32,
                items=[
                    ColumnDescription(
                        fSize=20,
                        fColumnType=ColumnType.kSplitReal32,
                        fBitsOnStorage=32,
                        fFieldID=0,
                        fFlags=0,
                        fRepresentationIndex=0,
                        fFirstElementIndex=None,
                        fMinValue=None,
                        fMaxValue=None,
                    )
                ],
            ),
            aliasColumnDescriptions=ListFrame(fSize=12, items=[]),
            extraTypeInformations=ListFrame(fSize=12, items=[]),
        )

        footer_a = rntuple_a.get_footer(fetch_data)
        assert footer_a == FooterEnvelope(
            typeID=2,
            length=148,
            checksum=16904131729352343975,
            featureFlags=RFeatureFlags(flags=0),
            headerChecksum=1772847515747675522,
            schemaExtension=SchemaExtension(
                fSize=56,
                fieldDescriptions=ListFrame(fSize=12, items=[]),
                columnDescriptions=ListFrame(fSize=12, items=[]),
                aliasColumnDescriptions=ListFrame(fSize=12, items=[]),
                extraTypeInformations=ListFrame(fSize=12, items=[]),
            ),
            clusterGroups=ListFrame(
                fSize=60,
                items=[
                    ClusterGroup(
                        fSize=48,
                        fMinEntryNumber=0,
                        fEntrySpan=100,
                        fNClusters=1,
                        pagelistLink=REnvelopeLink(
                            length=124, locator=StandardLocator(size=86, offset=597)
                        ),
                    )
                ],
            ),
        )

        page_location_lists_a = footer_a.get_pagelists(fetch_data)
        assert page_location_lists_a == [
            PageListEnvelope(
                typeID=3,
                length=124,
                checksum=748677678342101309,
                headerChecksum=1772847515747675522,
                clusterSummaries=ListFrame(
                    fSize=36,
                    items=[
                        ClusterSummary(
                            fSize=24, fFirstEntryNumber=0, fNEntriesAndFeatureFlag=100
                        )
                    ],
                ),
                pageLocations=ListFrame(
                    fSize=64,
                    items=[
                        ListFrame(
                            fSize=52,
                            items=[
                                PageLocations(
                                    fSize=40,
                                    items=[
                                        RPageDescription(
                                            fNElements=-100,
                                            locator=StandardLocator(
                                                size=138, offset=409
                                            ),
                                        )
                                    ],
                                    elementoffset=0,
                                    compressionsettings=505,
                                )
                            ],
                        )
                    ],
                ),
            )
        ]

        rntuple_b = keylist["B"].read_object(fetch_data, ROOT3a3aRNTuple)
        assert rntuple_b == ROOT3a3aRNTuple(
            fVersionEpoch=1,
            fVersionMajor=0,
            fVersionMinor=0,
            fVersionPatch=0,
            fSeekHeader=1542,
            fNBytesHeader=111,
            fLenHeader=171,
            fSeekFooter=2037,
            fNBytesFooter=82,
            fLenFooter=148,
            fMaxKeySize=1073741824,
        )

        header_b = rntuple_b.get_header(fetch_data)
        assert header_b == HeaderEnvelope(
            typeID=1,
            length=171,
            checksum=14068653553654343426,
            featureFlags=RFeatureFlags(flags=0),
            fName=RString(fString=b"B"),
            fDescription=RString(fString=b""),
            fLibrary=RString(fString=b"ROOT v6.35.01"),
            fieldDescriptions=ListFrame(
                fSize=65,
                items=[
                    FieldDescription(
                        fSize=53,
                        fFieldVersion=0,
                        fTypeVersion=0,
                        fParentFieldID=0,
                        fStructuralRole=0,
                        fFlags=0,
                        fFieldName=RString(fString=b"g"),
                        fTypeName=RString(fString=b"std::int32_t"),
                        fTypeAlias=RString(fString=b""),
                        fFieldDescription=RString(fString=b""),
                        fArraySize=None,
                        fSourceFieldID=None,
                        fTypeChecksum=None,
                    )
                ],
            ),
            columnDescriptions=ListFrame(
                fSize=32,
                items=[
                    ColumnDescription(
                        fSize=20,
                        fColumnType=ColumnType.kSplitInt32,
                        fBitsOnStorage=32,
                        fFieldID=0,
                        fFlags=0,
                        fRepresentationIndex=0,
                        fFirstElementIndex=None,
                        fMinValue=None,
                        fMaxValue=None,
                    )
                ],
            ),
            aliasColumnDescriptions=ListFrame(fSize=12, items=[]),
            extraTypeInformations=ListFrame(fSize=12, items=[]),
        )

        footer_b = rntuple_b.get_footer(fetch_data)
        assert footer_b == FooterEnvelope(
            typeID=2,
            length=148,
            checksum=17038928962946065552,
            featureFlags=RFeatureFlags(flags=0),
            headerChecksum=14068653553654343426,
            schemaExtension=SchemaExtension(
                fSize=56,
                fieldDescriptions=ListFrame(fSize=12, items=[]),
                columnDescriptions=ListFrame(fSize=12, items=[]),
                aliasColumnDescriptions=ListFrame(fSize=12, items=[]),
                extraTypeInformations=ListFrame(fSize=12, items=[]),
            ),
            clusterGroups=ListFrame(
                fSize=60,
                items=[
                    ClusterGroup(
                        fSize=48,
                        fMinEntryNumber=0,
                        fEntrySpan=100,
                        fNClusters=1,
                        pagelistLink=REnvelopeLink(
                            length=124, locator=StandardLocator(size=86, offset=1909)
                        ),
                    )
                ],
            ),
        )

        page_location_lists_b = footer_b.get_pagelists(fetch_data)
        assert page_location_lists_b == [
            PageListEnvelope(
                typeID=3,
                length=124,
                checksum=674435399773528910,
                headerChecksum=14068653553654343426,
                clusterSummaries=ListFrame(
                    fSize=36,
                    items=[
                        ClusterSummary(
                            fSize=24, fFirstEntryNumber=0, fNEntriesAndFeatureFlag=100
                        )
                    ],
                ),
                pageLocations=ListFrame(
                    fSize=64,
                    items=[
                        ListFrame(
                            fSize=52,
                            items=[
                                PageLocations(
                                    fSize=40,
                                    items=[
                                        RPageDescription(
                                            fNElements=-100,
                                            locator=StandardLocator(
                                                size=164, offset=1695
                                            ),
                                        )
                                    ],
                                    elementoffset=0,
                                    compressionsettings=505,
                                )
                            ],
                        )
                    ],
                ),
            )
        ]
