#!"C:\Users\Mahad\Dropbox\college\Sophomore\Second Semester\CS 382\Assignments\Assignment 3\Chord\chord\DC-2.0\venv\Scripts\python.exe"
# EASY-INSTALL-ENTRY-SCRIPT: 'pip==10.0.1','console_scripts','pip3.7'
__requires__ = 'pip==10.0.1'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('pip==10.0.1', 'console_scripts', 'pip3.7')()
    )
