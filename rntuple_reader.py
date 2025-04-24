# ruff: noqa: T201
from __future__ import annotations

from pathlib import Path

from rootfilespec.bootstrap import ROOTFile
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.bootstrap.RPage import RPage
from rootfilespec.dynamic import streamerinfo_to_classes
from rootfilespec.structutil import ReadBuffer

if __name__ == "__main__":
    initial_read_size = 512
    # path = Path("../TTToSemiLeptonic_UL18JMENanoAOD-zstd.root")
    path = Path("tests/RNTuple.root")
    print(f"\033[1;36mReading '{path}'...\n\033[0m")
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            """Fetches data from a file at a specified position and size.

            Args:
                seek (int): The position in the file to start reading from.
                size (int): The number of bytes to read from the file.

            Returns:
                ReadBuffer: A buffer containing the read data, along with the seek position and an offset of 0.
            """
            # print(f"\033[3;33mfetch_data {seek=} {size=}\033[0m")
            filehandle.seek(seek)
            return ReadBuffer(memoryview(filehandle.read(size)), seek, 0)

        # Get TFile Header
        buffer = fetch_data(0, initial_read_size)
        file, _ = ROOTFile.read(buffer)
        print(f"\t{file}\n")

        def fetch_cached(seek: int, size: int):
            # print(f"\033[3;33mfetch_cached {seek=} {size=}\033[0m")
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            print("Didn't find data in initial read buffer, fetching from file")
            return fetch_data(seek, size)

        # Get TFile object (root TDirectory)
        tfile = file.get_TFile(fetch_cached)
        print(f"\t{tfile}\n")

        # usually the directory tkeylist and the streamer info are adjacent at the end of the file
        keylist_start = tfile.rootdir.fSeekKeys
        keylist_stop = keylist_start + tfile.rootdir.header.fNbytesKeys
        print(f"KeyList at {keylist_start}:{keylist_stop}")
        streaminfo_start = file.header.fSeekInfo
        streaminfo_stop = streaminfo_start + file.header.fNbytesInfo
        print(f"StreamerInfo at {streaminfo_start}:{streaminfo_stop}")
        print(f"End of file at {file.header.fEND}")

        # Get TKeyList (List of all TKeys in the TDirectory)
        keylist = tfile.get_KeyList(fetch_data)

        # Print TKeyList
        msg = "\tTKey List Summary:\n"
        for name, key in keylist.items():
            msg += f"\t\tName: {name}; Class: {key.fClassName.fString}\n"
        print(msg)

        # Get TStreamerInfo (List of classes used in the file)
        streamerinfo = file.get_StreamerInfo(fetch_data)
        classes = streamerinfo_to_classes(streamerinfo)
        with Path("classes.py").open("w") as f:
            f.write(classes)
        # exec(classes, globals())

        ########################################################################################################################
        print(f"\033[1;31m\n/{'-' * 44} Begin Reading RNTuples {'-' * 44}/ \033[0m")

        #### Get RNTuple Info
        # Only RNTuple Anchor TKeys are visible (i.e. in TKeyList); ClassName = ROOT::RNTuple
        # anchor_keylist = [key for key in keylist.values() if key.fClassName.fString == b'ROOT::RNTuple']
        for name, tkey in keylist.items():
            # Check for RNTuple Anchors
            if tkey.fClassName.fString == b"ROOT::RNTuple":
                print(
                    f"\033[1;33m\n{'-' * 34} Begin Reading RNTuple: '{name}' {'-' * 34}\033[0m"
                )
                # print(f"\t{tkey}")

                ### Get RNTuple Anchor Object
                anchor = tkey.read_object(fetch_data, ROOT3a3aRNTuple)

                # Print attributes of the RNTuple Anchor
                print(f"{anchor=}\n")
                # anchor.print_info()

                ### Get the RNTuple Header Envelope from the Anchor
                # anchor.get_header(fetch_data)

                ### Get the RNTuple Footer Envelope from the Anchor
                footer = anchor.get_footer(fetch_data)

                # Print attributes of the RNTuple Footer
                print(f"{footer=}\n")
                # footer.print_info()

                ### Get the RNTuple Page List Envelopes from the Footer Envelope
                page_location_lists = footer.get_pagelist(fetch_data)

                # Print attributes of the RNTuple Page List Envelopes
                for i, page_location_list in enumerate(page_location_lists):
                    print(f"Page List Envelope {i}:")
                    print(f"\t{page_location_list=}\n")
                    # page_location_list.print_info()
                ### Get the RNTuple Pages from the Page List Envelopes

                cluster_column_page_lists: list[list[list[RPage]]] = []
                for page_location_list in page_location_lists:
                    pages = page_location_list.get_pages(fetch_data)
                    # print(f"\n{pages=}\n")
                    cluster_column_page_lists.append(pages)

                # Print attributes of the RNTuple Pages
                for i_cluster, column_page_lists in enumerate(
                    cluster_column_page_lists
                ):
                    for i_column, page_list in enumerate(column_page_lists):
                        for i_page, page in enumerate(page_list):
                            print(
                                f"Cluster {i_cluster}, Column {i_column}, Page {i_page}:"
                            )
                            print(f"\t{page=}\n")
                            # page.print_info()

                print(
                    f"\033[1;33m{'-' * 34} Done Reading RNTuple: '{name}' {'-' * 34}\033[0m"
                )

    print(f"\n\033[1;32mClosing '{path}'\n\033[0m")
    # quit()

    # print(f"TStreamerInfo Summary:")
    # for item in streamerinfo.items:
    #     if isinstance(item, TStreamerInfo):
    #         print(f"\t{item.b_named.fName.fString}")
    #         for obj in item.fObjects.objects:
    #             # print(f"\t\t{obj.b_element.b_named.fName.fString}: {obj.b_element.b_named.b_object}")
    #             print(f"\t\t{obj.b_element.b_named.fName.fString}")
