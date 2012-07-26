#!/usr/bin/python

import tkp_lib.database as db
import tkp_lib.dbcalibfield as cal

conn = db.connection()

icatname = 'VLSS'
itheta = 4.5
ira = 217
idecl = 53

cal.brightestInField(conn,icatname,itheta,ira,idecl)


"""
cat_id = 4

idecldms = '+53:00:00.00'


#ra = ['04:04:00.00','02:56:00.00','01:48:00.00','00:40:00.00','23:32:00.00','22:34:00.00','21:16:00.00','20:08:00.00','19:00:00.00','17:52:00.00','16:44:00.00','15:36:00.00','14:28:00.00','13:20:00.00','12:12:00.00','11:04:00.00','09:56:00.00','08:48:00.00','07:40:00.00','06:32:00.00','05:24:00.00','04:16:00.00']


ra = ['04;16:00.00','05;24:00.00','06;32:00.00','07;40:00.00','08:48:00.00','09:56:00.00','11:04:00.00','12:12:00.00','13:20:00.00','14:28:00.00','15:36:00.00','16:44:00.00','17:52:00.00','19:00:00.00','20:08:00.00','21:16:00.00','22:34:00.00']


itheta = 4.5

print ra[9]
#for i in range(len(ra)):
cal.printBrightestSourceInField(conn,9,cat_id,ra[9],idecldms,itheta)
"""

