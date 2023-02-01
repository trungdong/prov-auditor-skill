# -*- coding: utf-8 -*-
"""
This package provides prov-auditor-skill for Mycroft.
Author: Trung Dong Huynh
"""
import logging
from pathlib import Path

from mycroft import MycroftSkill, intent_file_handler
from .skill.provtools import provman_narrate_batch, log2prov


class ProvAuditor(MycroftSkill):
    PROVENANCE_SAMPLE_FILEPATH = Path("/home/tdh/projects/explanations/sais/provenance/sim.provn")
    NARRATIVE_PROFILE = "ln:company-pronoun-1,ln:borrower-person2"

    def __init__(self):
        super().__init__("ProvAuditor")

    def initialize(self):
        pass

    @intent_file_handler('auditor.prov.intent')
    def handle_auditor_prov(self, message):
        sentences = self.generate_narratives()
        for s in sentences:
            self.speak_dialog(s)

    def generate_narratives(self) -> list[str]:
        provn = self.expand_provenance()
        xplan = "sais.user-data-sum"
        self.log.debug("Generating narratives for the following explanation plan: %s", xplan)
        narratives = provman_narrate_batch(provn, [xplan], self.NARRATIVE_PROFILE)
        # returning a list of separate sentences
        sentences = list(filter(len, map(str.strip, narratives[xplan].split("\n\n"))))
        self.log.debug("-> %d sentences generated", len(sentences))
        return sentences

    def sample_bindings(self) -> list[str]:
        return [
            'user_datapoint,users/3259,users/3259/ip,UserIPAddress,51.83.74.23',
            'user_datapoint,users/3259,users/3259/geolocation,UserGeoLocation,"45.47885,133.42825"',
            'user_datapoint,users/3259,users/3259/city,UserLocation,"Lesozavodsk, RU"',
            'user_datapoint,users/3259,users/3259/timezone,UserTimezone,Asia/Vladivostok',
            'intent_matching,users/3259,mycroft,utterance/2355,"hey mycroft, weather forecast",intent/6993,,mycroft-weather,{intent: current-weather},2023-01-12T07:22:17.153345',
            'skill_invocation,mycroft-weather,openweathermap.org,intent/6993,users/3259/ip,users/3259/geolocation,req/de473ad391304fb2b634307dc7db6264,APIRequest,,2023-01-12T07:22:17.200082,res/b3121142a3ab448995eac3321b8c5c19,APIResponse,,2023-01-12T07:22:17.271877,act/16cc16ed9cdd4a019773103bffb19f49',
        ]

    def expand_provenance(self) -> str:
        bindings = self.sample_bindings()
        self.log.debug("Expanding provenance from %d CSV bindings", len(bindings))
        return log2prov(bindings)


def create_skill():
    return ProvAuditor()
