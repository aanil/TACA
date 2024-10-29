import os
import re

from taca.illumina.Standard_Runs import Standard_Run

IDT_UMI_PAT = re.compile("([ATCG]{4,}N+$)")


class NovaSeqXPlus_Run(Standard_Run):
    def __init__(self, run_dir, software, configuration):
        super().__init__(run_dir, software, configuration)
        self._set_sequencer_type()
        self._set_run_type()
        self._copy_samplesheet()

    def _set_sequencer_type(self):
        self.sequencer_type = "NovaSeqXPlus"

    def _set_run_type(self):
        self.run_type = "NGI-RUN"

    def _current_year(self):
        """Method needed to extract year from rundir name, since year contains 4 digits
        on NovaSeqXPlus while previously it was 2."""
        return self.id[0:4]

    def _revcomp(self, seq: str) -> str:
        return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]

    def _generate_samplesheet_subset(
        self,
        ssparser,
        samples_to_include,
        runSetup,
        software,
        sample_type,
        index1_size,
        index2_size,
        base_mask,
        CONFIG,
    ):
        output = ""
        # Prepare index cycles
        index_cycles = [0, 0]
        for read in runSetup:
            if read["IsIndexedRead"] == "Y":
                if int(read["Number"]) == 2:
                    index_cycles[0] = int(read["NumCycles"])
                else:
                    index_cycles[1] = int(read["NumCycles"])
        # Header
        output += f"[Header]{os.linesep}"
        for field in sorted(ssparser.header):
            output += f"{field.rstrip()},{ssparser.header[field].rstrip()}"
            output += os.linesep
        # Settings for BCL Convert
        if software == "bclconvert":
            output += f"[Settings]{os.linesep}"
            output += "OverrideCycles,{}{}".format(";".join(base_mask), os.linesep)
            if any("U" in bm for bm in base_mask):
                output += f"TrimUMI,0{os.linesep}"

            if CONFIG.get("bclconvert"):
                if CONFIG["bclconvert"].get("settings"):
                    # Put common settings
                    if CONFIG["bclconvert"]["settings"].get("common"):
                        for setting in CONFIG["bclconvert"]["settings"]["common"]:
                            for k, v in setting.items():
                                output += f"{k},{v}{os.linesep}"
                    # Put special settings:
                    if sample_type in CONFIG["bclconvert"]["settings"].keys():
                        for setting in CONFIG["bclconvert"]["settings"][sample_type]:
                            for k, v in setting.items():
                                if (
                                    (
                                        k == "BarcodeMismatchesIndex1"
                                        and index1_size != 0
                                    )
                                    or (
                                        k == "BarcodeMismatchesIndex2"
                                        and index2_size != 0
                                    )
                                    or "BarcodeMismatchesIndex" not in k
                                ):
                                    output += f"{k},{v}{os.linesep}"
        # Data
        output += f"[Data]{os.linesep}"
        datafields = []
        for field in ssparser.datafields:
            datafields.append(field)
        output += ",".join(datafields)
        output += os.linesep
        for line in ssparser.data:
            sample_name = line.get("Sample_Name") or line.get("SampleName")
            lane = line["Lane"]
            noindex_flag = False
            if lane in samples_to_include.keys():
                if sample_name in samples_to_include.get(lane):
                    line_ar = []
                    for field in datafields:
                        # Case with NoIndex
                        if field == "index" and "NOINDEX" in line["index"].upper():
                            line[field] = (
                                "T" * index_cycles[0] if index_cycles[0] != 0 else ""
                            )
                            noindex_flag = True
                        if field == "index2" and noindex_flag:
                            line[field] = (
                                "A" * index_cycles[1] if index_cycles[1] != 0 else ""
                            )
                            noindex_flag = False
                        # Case of IDT UMI
                        if (
                            field == "index" or field == "index2"
                        ) and IDT_UMI_PAT.findall(line[field]):
                            line[field] = line[field].replace("N", "")
                        # Convert Index 2 into RC for NovaSeqXPlus
                        if field == "index2":
                            line[field] = self._revcomp(line[field])
                        line_ar.append(line[field])
                    output += ",".join(line_ar)
                    output += os.linesep
        return output
