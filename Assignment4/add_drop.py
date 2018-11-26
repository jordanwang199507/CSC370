# add_drop.py
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
		if len(row) != 5:
			print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)

			break
		add_or_drop,student_id,student_name,course_code,term = row
		
		if (add_or_drop == 'DROP'):
			try:
				cursor.execute("delete from enrollments where id = %s and code = %s and term_code = %s;", 
					(student_id, course_code, term))
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

		elif (add_or_drop == 'ADD'):

			try:
				cursor.execute("insert into students values(%s, %s);", 
					(student_id, student_name))
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
				cursor.execute("insert into enrollments values(%s, %s, %s);", 
					(student_id, course_code, term))
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

		else:
			print ('incorrect add_or_drop value, statement not executed.')

cursor.close()
conn.close()
