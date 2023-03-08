# -*- coding: utf-8 -*-
"""
This package provides an interface to command-line tools.
Author: Trung Dong Huynh
"""
import json
import logging
import os
import subprocess
import tempfile
from typing import Sequence


PROVCONVERT_PATH = "provconvert"
PROVMAN_PATH = "provmanagement"

logger = logging.getLogger(__name__)


def call_external_tool(executable, arguments, pipe_input=None, timeout=None, env=None) -> str:
    """Call the external command-line tool at `executable` with the provided arguments.
    Args:
        executable:
        arguments:
        pipe_input:
        timeout:
    Returns: The output from the execution of the tool.
    """
    args = list(map(str, (executable, *arguments)))
    logger.debug("Calling command: %s", " ".join(args))
    p = subprocess.Popen(
        args,
        stdin=subprocess.PIPE if pipe_input is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    b_input = pipe_input.encode("utf8") if pipe_input is not None else None
    stdout, stderr = p.communicate(input=b_input, timeout=timeout)
    if p.returncode != 0:
        stdout, stderr = stdout.decode(), stderr.decode()
        logger.debug(
            "%s returns non-zero code (%d)\nOutput: %s\nError: %s",
            executable,
            p.returncode,
            stdout,
            stderr,
        )
        raise subprocess.CalledProcessError(p.returncode, args, stdout, stderr)
    return stdout.decode()


def provman_narrate_batch(provn: str, templates: Sequence[str], profile: str = None) -> dict[str, str]:
    """Explain command specific to the credit scoring scenario.
    """
    batch_templates = ",".join(f'[{template}]' for template in templates)

    with tempfile.TemporaryDirectory() as tmpdirname:
        output_filepath = os.path.join(tmpdirname, "narrative.provn")
        narrative_filepath = os.path.join(tmpdirname, "narrative.json")
        arguments = [
            "explain",
            "--infile", "-",
            "--outfile", output_filepath,
            "--text", narrative_filepath,
            # -s $(OUTPUTS)/narrative-simplenlg.txt
            "--language", "/home/tdh/projects/explanations/sais/xplan/sais-template-library.json",
            f"--batch-templates={batch_templates}",
            "-X", "0",  # do not include HTML mark-ups
        ]
        if profile is not None:
            arguments.append("--profile=" + profile)

        call_external_tool(PROVMAN_PATH, arguments, pipe_input=provn)

        with open(narrative_filepath) as f:
            return json.load(f)


def log2prov(bindings_str: str) -> str:
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["CLASSPATH_PREFIX"] = "/home/tdh/.m2/repository/org/openprovenance/sais/templates_l2p/0.1.0/templates_l2p-0.1.0.jar:/home/tdh/.m2/repository/org/openprovenance/sais/templates_cli/0.1.0/templates_cli-0.1.0.jar"
        output_filepath = os.path.join(tmpdirname, "expansion.provn")
        arguments = [
            "--infile", "-",
            "--log2prov", "org.openprovenance.sais.Init",
            "--outfile", output_filepath,
        ]

        call_external_tool(PROVCONVERT_PATH, arguments, pipe_input=bindings_str, env=os.environ)

        with open(output_filepath) as f:
            return f.read()
