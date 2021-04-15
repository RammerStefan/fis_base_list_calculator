# Programm to calculate the FIS base-List with the old (2020), non Covid Calculation Rules
# Author: Stefan Rammer - Ski Austria Technology
# Mail: stefan.rammer@oesv.at

import pymsql
import time

from Classes.DbHandler import DbHandler

db_source = pymsql.connect("localhost", "root", "supertrixi22!", "karriere_analyse_2021")
source = DbHandler(db_source)

source.getVersion()