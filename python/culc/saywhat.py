import re

class saywhat(object):

    def __init__(self):
        self.pyg = "ay"

    def communicate(self, statement):
        if not isinstance(statement, basestring):
            return ""

        statement = statement.lower()

        pattern = re.compile('[a,e,i,o,u]')

        if statement.startswith('y'):
            return statement + 'y' + self.pyg

        first_vowel = pattern.search(statement)
        if not first_vowel:
            return statement + self.pyg

        first_vowel = first_vowel.group()

        if statement.find(first_vowel) == 0:
            return statement + 'y' + self.pyg

        first, second = statement.split(first_vowel, 1)
        return first_vowel + second + first + self.pyg

