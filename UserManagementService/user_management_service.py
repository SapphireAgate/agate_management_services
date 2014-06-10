#!/usr/bin/python

import time
import socket
import sys
import MySQLdb as mdb

HOST = 'dunbar.cs.washington.edu'   # Symbolic name meaning the local host
PORT = 24068    # Arbitrary non-privileged port

def execute_query(message):

    try:
	con = mdb.connect('localhost', 'root', 'root', 'UserManagementService')
	print message
	cur = con.cursor()
	#message="SELECT * FROM Users where username='adrdiana' AND password='12345678'"
	cur.execute(message)
	return cur.fetchone()

    except mdb.Error, e:
	print "Error %d: %s" % (e.args[0],e.args[1])
	return -1

def start_service():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    print 'Socket created'

    try:
	s.bind((HOST, PORT))
    except socket.error , msg:
    	print 'Bind failed. Error code: ' + str(msg[0]) + 'Error message: ' + msg[1]
    	sys.exit()

    print 'Socket bind complete'
    s.listen(1)
    print 'Socket now listening'

    while True:
	(conn, addr) = s.accept()
	print 'Connected with ' + addr[0] + ':' + str(addr[1])

	# RECEIVE DATA
	data = conn.recv(1024)
	reply = execute_query(data)

	if reply is None:
    	    conn.send('-1')
	else:
	    conn.send(str(reply[0]))  
	conn.close() 

if __name__ == "__main__":
    start_service()
