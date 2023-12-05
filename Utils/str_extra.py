import re


def add_tabs(string, n_tabs=1, tab_string='\t'):
    if type(string) is not str:
        string = str(string)
    tabs = ''.rjust(n_tabs, tab_string)
    return tabs + re.sub(r'\n', f"\n{tabs}", string)
