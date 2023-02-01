from mycroft import MycroftSkill, intent_file_handler


class ProvAuditor(MycroftSkill):
    def __init__(self):
        MycroftSkill.__init__(self)

    @intent_file_handler('auditor.prov.intent')
    def handle_auditor_prov(self, message):
        self.speak_dialog('auditor.prov')


def create_skill():
    return ProvAuditor()

