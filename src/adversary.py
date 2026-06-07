#!/usr/bin/env python3
# adversary.py - runs the same node but with malicious flag
import os
os.system("python node.py --malicious " + " ".join(sys.argv[1:]))