"""
(c) 2014 Arts Alliance Media

Tree like representation of a stack trace.
"""


def count_spaces(string):
    """Counts the number of initial spaces in a string.

    Returns:
        A tuple with the number of spaces and the string excluding the spaces.
    """
    count = 0
    for ch in string:
        if ch == ' ':
            count += 1
        else:
            break
    return (count, string[count:])


class StackTree(object):
    """Represents a stack trace in a tree."""
    def __init__(self, level, value):
        self._children = []
        self._level = level
        self._value = value
        self._store = {}

    def append(self, level, value):
        if level == self._level + 1:
            node = StackTree(level, value)
            self._children.append(node)
        else:
            self._children[-1].append(level, value)

    def get(self, name):
        return self._store[name]

    def level(self):
        return self._level

    def reverse_traverse(self, function):
        function(self)
        for c in reversed(self._children):
           c.reverse_traverse(function)

    def store(self, name, value):
        self.store_[name] = value

    def traverse(self, function):
        function(self)
        for c in self._children:
           c.traverse(function)

    def value(self):
        return self.value_

def build_from_file(trace):
    line = trace.readline().rstrip()
    (top, value) = count_spaces(line)
    assert top == 0, "build_from_file requires a top level stack trace."
    root = StackTree(top, value)
    for line in trace:
        line = line.rstrip()
        (level, value) = count_spaces(line)
        root.append(level, value)
    return root
