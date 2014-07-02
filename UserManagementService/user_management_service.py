#!/usr/bin/python

import time
import socket
import threading
import sys
import MySQLdb as mdb

HOST = 'dunbar.cs.washington.edu'   # Symbolic name meaning the local host
PORT = 24068    # Arbitrary non-privileged port

def execute_query(client,n):
	con = mdb.connect('localhost', 'root', 'root', 'UserManagementService')
	while True:
		try:
			# RECEIVE DATA
			message = client.recv(1024)

			print message
			cur = con.cursor()
			#message="SELECT * FROM Users where username='adrdiana' AND password='12345678'"
			cur.execute(message)

			reply = cur.fetchone()
			if reply is None:
				client.send('-1')
			else:
				client.send(str(reply[0]))

		except mdb.Error, e:
			print "Error %d: %s" % (e.args[0],e.args[1])
			client.close()
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

		t = threading.Thread(target=execute_query, args = (conn,1))
		t.daemon = True
		t.start()

if __name__ == "__main__":
    start_service()



