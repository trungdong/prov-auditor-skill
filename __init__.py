# -*- coding: utf-8 -*-
"""
This package provides prov-auditor-skill for Mycroft.
Author: Trung Dong Huynh
"""
import csv
from datetime import datetime, timedelta
import json
from collections import defaultdict, namedtuple
from io import StringIO
from pathlib import Path
import random
from typing import Optional
import uuid

from mycroft import MycroftSkill, intent_file_handler
from mycroft.identity import IdentityManager
from mycroft.messagebus.message import Message
from mycroft.session import Session, SessionManager
from .skill.provtools import provman_narrate_batch, log2prov


# Bindings types
IntentMatchingBinding = namedtuple(
    "IntentMatchingBinding",
    ["isA", "user", "assistant", "utterance", "value", "intent", "intent_type", "skill", "intent_data", "timestamp"]
)
UserDatapointBinding = namedtuple(
    "UserDatapointBinding",
    ["isA", "user", "user_datapoint", "data_type", "value"]
)
SkillInvocationBinding = namedtuple(
    "SkillInvocationBinding",
    ["isA", "skill", "service", "intent", "user_ip", "user_datapoint",
     "request", "req_type", "req_data", "req_timestamp",
     "response", "res_type", "res_data", "res_timestamp",
     "service_call"]
)


def random_delay(around_seconds: float) -> timedelta:
    return timedelta(seconds=random.triangular(0, 2 * around_seconds, around_seconds))


class ProvAuditor(MycroftSkill):
    NARRATIVE_PROFILE = "ln:company-pronoun-1,ln:borrower-person2"
    PATH_BINDINGS = "bindings"

    def __init__(self):
        super().__init__("ProvAuditor")
        self.identity = IdentityManager.get()
        self.bindings: list[tuple] = []
        self.path_bindings: Path = Path(self.file_system.path) / self.PATH_BINDINGS
        self.session: Optional[Session] = None
        self.id_counters: Optional[dict[str, int]] = None
        self.utterance_id_cache: dict[tuple, str] = dict()
        self.geolocation_id_cache: dict[tuple, str] = dict()
        self.intent_id_cache: dict[str, str] = dict()

    def initialize(self):
        self.add_event("skill.prov_auditor.log_intent", self.handler_log_intent)
        self.add_event("skill.prov_auditor.log_bindings", self.handler_log_bindings)
        self.add_event("recognizer_loop:utterance", self.handler_utterance)
        self.add_event("speak", self.handler_speak)

    def handler_speak(self, message):
        self.log.info(message.data)
        self.log.info("Spoken by %s: %s", message.data["meta"]["skill"], message.data["utterance"])

    def shutdown(self):
        self.persist_bindings()

    def handler_utterance(self, message):
        self.check_active_session()
        utterance_id = self.get_id("utterance")
        self.utterance_id_cache[tuple(message.data["utterances"])] = utterance_id
        self.log.info("Utterance %s: %s", utterance_id, message.data["utterances"])

    def handler_log_intent(self, message):
        timestamp = message.context["timestamp"]
        # deserialising the original intent message
        intent_msg: Message = Message.deserialize(message.data)
        utterance = intent_msg.data["utterance"]
        utterances = intent_msg.data["utterances"]
        self.log.info(intent_msg.data)
        skill_id, intent_type = intent_msg.msg_type.split(":")
        intent_data = intent_msg.data
        intent_id = self.get_id("intent")
        # remembering the last intent_id we saw for this skill
        self.intent_id_cache[skill_id] = intent_id
        # removing redundant information
        if "intent_type" in intent_data:
            del intent_data["intent_type"]
        del intent_data["utterance"]
        del intent_data["utterances"]
        if "__tags__" in intent_data:
            del intent_data["__tags__"]
        # log this binding
        self.bindings.append(
            IntentMatchingBinding(
                "intent_matching",
                self.get_user_id(),
                self.identity.uuid,
                self.utterance_id_cache[tuple(utterances)],
                utterance,
                intent_id,
                f"{skill_id}/{intent_type}",
                skill_id,
                json.dumps(intent_data),
                datetime.fromtimestamp(timestamp).isoformat(),
            )
        )
        self.log.info(self.bindings[-1])

    def handler_log_bindings(self, message):
        self.log.info(message.serialize())
        # TODO: check if geolocation data is present before retrieving them
        latitude = message.data["latitude"]
        longitude = message.data["longitude"]
        skill_id = message.context["sender"]
        request_identifier = "req/" + uuid.uuid4().hex
        response_identifier = "res/" + uuid.uuid4().hex
        service_url = message.context["service"]
        service_call_identifier = "act/" + uuid.uuid4().hex
        request_time = datetime.fromtimestamp(message.context["timestamp"])
        response_time = request_time + random_delay(0.4)
        self.bindings.append(
            SkillInvocationBinding(
                "skill_invocation",
                skill_id,
                service_url[8:],  # removing "https://"
                self.intent_id_cache.get(skill_id, None),
                None,  # TODO: IP address
                self.get_geolocation_id(latitude, longitude),
                request_identifier, "APIRequest", None, request_time.isoformat(),
                response_identifier, "APIResponse", None, response_time.isoformat(),
                service_call_identifier,
            )
        )
        self.log.info(self.bindings[-1])

    @intent_file_handler('auditor.prov.intent')
    def handle_auditor_prov(self, message):
        sentences = self.generate_narratives()
        if sentences:
            for s in sentences:
                self.speak_dialog(s)
        else:
            self.speak_dialog("No data was recorded in my log")

    def check_active_session(self) -> None:
        """
        Check for the active session and reset all the ID counters if a new session has started
        """
        session = SessionManager.get()
        if session is not self.session:
            self.persist_bindings()  # store all existing bindings before switching to the new session
            self.id_counters = defaultdict(int)  # resetting the ID counters
            self.utterance_id_cache = dict()  # forgetting previous utterances
            self.session = session  # remembering the current session

    def get_id(self, id_kind: str):
        self.id_counters[id_kind] += 1
        return f"{id_kind}/{self.session.session_id}/{self.id_counters[id_kind]}"

    def get_user_id(self) -> str:
        # TODO: Figure out how to do this
        return "users/3259"

    def get_user_data_id(self, data_id: str):
        return f"{self.get_user_id()}/{data_id}"

    def get_geolocation_id(self, latitude, longitude):
        geolocation_id = self.geolocation_id_cache.get((latitude, longitude), None)
        if geolocation_id is None:
            # Create a new ID and register it
            geolocation_id = f"{self.get_user_data_id('geolocation')}/{len(self.geolocation_id_cache) + 1}"
            self.geolocation_id_cache[(latitude, longitude)] = geolocation_id
            self.bindings.append(
                UserDatapointBinding(
                    "user_datapoint", self.get_user_id(), geolocation_id, "UserGeoLocation",
                    f"{latitude},{longitude}"
                )
            )
            self.log.info(self.bindings[-1])
        return geolocation_id

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

    def persist_bindings(self):
        # only proceed if there is some bindings to save
        if not self.bindings:
            return  # nothing to save
        # determine the right path
        session_ts = self.session.touch_time  # seconds since epoch
        session_dt = datetime.fromtimestamp(session_ts)
        folder_path = self.path_bindings / str(session_dt.year) / str(session_dt.month) / str(session_dt.day)
        if not folder_path.exists():
            folder_path.mkdir(parents=True, exist_ok=True)
        file_path = folder_path / f"{self.session.session_id}.csv"
        # write the bindings to a CSV file
        with file_path.open("a") as f:
            csvwriter = csv.writer(f)
            csvwriter.writerows(self.bindings)
            self.log.info("%d bindings saved to %s", len(self.bindings), file_path)
        self.bindings = list()  # forgetting all the current bindings

    def collect_bindings_lines(self) -> str:
        csv_lines = ""
        # read all stored bindings
        for filepath in self.path_bindings.glob("**/*.csv"):
            with filepath.open() as f:
                csv_lines += f.read()
        # append in-memory bindings
        if self.bindings:
            csv_lines += self.get_csv_bindings_str()
        return csv_lines

    def get_csv_bindings_str(self) -> str:
        f = StringIO()
        csvwriter = csv.writer(f)
        csvwriter.writerows(self.bindings)
        return f.getvalue()

    def expand_provenance(self) -> str:
        binding_lines = self.collect_bindings_lines()
        n_bindings_lines = binding_lines.count("\n")
        if not binding_lines.endswith("\n"):
            n_bindings_lines += 1
        self.log.info("Expanding provenance from %d CSV bindings", n_bindings_lines)
        self.log.info("The current CSV bindings:\n%s", binding_lines)
        return log2prov(binding_lines)


def create_skill():
    return ProvAuditor()
