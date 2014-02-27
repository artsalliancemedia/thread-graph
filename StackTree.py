"""
(c) 2014 Arts Alliance Media

Tree like representaion of a stack trace.
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
        self.children_ = []
        self.level_ = level
        self.value_ = value

    def append(self, level, value):
        if level == self.level_ + 1:
            node = StackTree(level, value)
            self.children_.append(node)
        else:
            self.children_[-1].append(level, value)

    def level(self):
        return self.level_

    def reverse_traverse(self, function):
        function(self)
        for c in reversed(self.children_):
           c.reverse_traverse(function)

    def value(self):
        return self.value_

def build_from_file(trace):
    line = trace.readline().rstrip()
    (top, value) = count_spaces(line)
    assert top == 0, "build_from_file requires a top lavel stack trace."
    root = StackTree(top, value)
    for line in trace:
        line = line.rstrip()
        (level, value) = count_spaces(line)
        root.append(level, value)
    return root
