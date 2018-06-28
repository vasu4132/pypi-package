import json

__author__ = 'nsandela@cisco.com'


class UTILS(object):

    def is_json(myjson):
        """Validate if given input is json string"""
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            # print e
            return False
        return True, json_object

    def validate_input(**data):
        """Validate if given input has mandatory keys"""
        MANDATORY_KEYS = ['RuleList']
        # print MANDATORY_KEYS
        # print data.keys()
        if all(k in data.keys() for k in MANDATORY_KEYS):
            # if MANDATORY_KEYS in data.keys():# check mandatory keys
            return True
        else:
            return False
