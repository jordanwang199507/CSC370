# create_courses.py
# CSC 370 - Spring 2018 - Starter code for Assignment 4
# Jordan (Yu-Lin) Wang
# V00786970
# B. Bird - 02/26/2018

import sys, csv, psycopg2

if len(sys.argv) < 2:
	print("Usage: %s <input file>",file=sys.stderr)
	sys.exit(0)
	
input_filename = sys.argv[1]

psql_user = 'yulin07'
psql_db = 'yulin07'
psql_password = 'V00786970'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()


with open(input_filename) as f:
	for row in csv.reader(f):
		if len(row) == 0:
			continue 
		if len(row) < 4:
			print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)

			break
		code, name, term, instructor, capacity = row[0:5]
		prerequisites = row[5:]

		try:
			cursor.execute("insert into courses values(%s);",(code,))
			conn.commit()
		except psycopg2.ProgrammingError as err:
			print ('Caught a ProgrammingError:',file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()
		except psycopg2.IntegrityError as err: 
			print("Caught an IntegrityError:",file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()
		except psycopg2.InternalError as err:  
			print("Caught an IntegrityError:",file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()

		try:
			cursor.execute("insert into course_offerings values(%s, %s, %s, %s, %s);", 
				(code, name, term, instructor, capacity))
			conn.commit()
		except psycopg2.ProgrammingError as err:
			print ('Caught a ProgrammingError:',file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()
		except psycopg2.IntegrityError as err: 
			print("Caught an IntegrityError:",file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()
		except psycopg2.InternalError as err:  
			print("Caught an IntegrityError:",file=sys.stderr)
			print(err,file=sys.stderr)
			conn.rollback()

		if (len(prerequisites) == 2):
			try:
				cursor.execute("insert into prerequisites values(%s, %s, %s);", 
					(code, term, prerequisites[1]))
				conn.commit()
			except psycopg2.ProgrammingError as err:
				print ('Caught a ProgrammingError:',file=sys.stderr)
				print(err,file=sys.stderr)
				conn.rollback()
			except psycopg2.IntegrityError as err: 
				print("Caught an IntegrityError:",file=sys.stderr)
				print(err,file=sys.stderr)
				conn.rollback()
			except psycopg2.InternalError as err:  
				print("Caught an IntegrityError:",file=sys.stderr)
				print(err,file=sys.stderr)
				conn.rollback()

		if (len(prerequisites) > 0):
			try:
				cursor.execute("insert into prerequisites values(%s, %s, %s);", 
					(code, term, prerequisites[0]))
				conn.commit()
			except psycopg2.ProgrammingError as err:
				print ('Caught a ProgrammingError:',file=sys.stderr)
				print(err,file=sys.stderr)
				conn.rollback()
			except psycopg2.IntegrityError as err: 
				print("Caught an IntegrityError:",file=sys.stderr)
				print(err,file=sys.stderr)
				conn.rollback()
			except psycopg2.InternalError as err:  
				print("Caught an IntegrityError:",file=sys.stderr)
				print(err,file=sys.stderr)
				conn.rollback()


cursor.close()
conn.close()

