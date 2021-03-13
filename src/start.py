# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 20:48:22 2021

@author: Sebastian
"""


import sys
import argparse
import common.globalcontainer as glob


ap = argparse.ArgumentParser()
ap.add_argument("-c", "--config", required=True, help="path to config file")

args = vars(ap.parse_args())

configFile = args["config"]

gc = glob.GlobalContainer(configFile)

print (gc.influx_url)

