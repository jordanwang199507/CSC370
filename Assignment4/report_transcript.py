# report_transcript.py
# CSC 370 - Spring 2018 - Starter code for Assignment 4
# Jordan (Yu-Lin) Wang
# V00786970
# The code below generates a mockup of the output of report_transcript.py
# as specified in the assignment. You can copy and paste the functions in this
# program into your solution to ensure the correct formatting.
#
# B. Bird - 02/26/2018

import psycopg2, sys

def print_header(student_id, student_name):
	print("Transcript for %s (%s)"%(str(student_id), str(student_name)) )
	
def print_row(course_term, course_code, course_name, grade):
	if grade is not None:
		print("%6s %10s %-35s   GRADE: %s"%(str(course_term), str(course_code), str(course_name), str(grade)) )
	else:
		print("%6s %10s %-35s   (NO GRADE ASSIGNED)"%(str(course_term), str(course_code), str(course_name)) )

psql_user = 'yulin07'
psql_db = 'yulin07'
psql_password = 'V00786970'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()

if len(sys.argv) < 2:
	print('Usage: %s <student id>'%sys.argv[0], file=sys.stderr)
	sys.exit(0)
	
student_id = sys.argv[1]

cursor.execute("""select student_name from students where id = %s;""", (student_id,))
row = cursor.fetchone()
if (row is None):
	print ("Error: Student does not exist.")
	exit()

print_header(student_id, row[0])

cursor.execute("""with x as (select ID, term_code, code from enrollments
		               except
	                     select ID, term_code, code from grades),
                       y as (select term_code, code, student_name, null as grade from x
		               natural join course_offerings 
		             where ID = %s)
select term_code, code, student_name, grade from course_offerings
	natural join grades natural join enrollments 
	where id = %s
	union 
select term_code, code, student_name, null as grade from y""", (student_id, student_id))

while True:
	row = cursor.fetchone()
	if row is None:
		break
	print_row(row[0], row[1], row[2], row[3])

cursor.close()
conn.close()
