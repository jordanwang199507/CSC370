# report_classlist.py
# CSC 370 - Spring 2018 - Starter code for Assignment 4
# Jordan (Yu-Lin) Wang
# V00786970
# The code below generates a mockup of the output of report_classlist.py
# as specified in the assignment. You can copy and paste the functions in this
# program into your solution to ensure the correct formatting.
#
# B. Bird - 02/26/2018

import psycopg2, sys

def print_header(course_code, course_name, term, instructor_name):
	print("Class list for %s (%s)"%(str(course_code), str(course_name)) )
	print("  Term %s"%(str(term), ) )
	print("  Instructor: %s"%(str(instructor_name), ) )
	
def print_row(student_id, student_name, grade):
	if grade is not None:
		print("%10s %-25s   GRADE: %s"%(str(student_id), str(student_name), str(grade)) )
	else:
		print("%10s %-25s"%(str(student_id), str(student_name),) )

def print_footer(total_enrolled, max_capacity):
	print("%s/%s students enrolled"%(str(total_enrolled),str(max_capacity)) )

psql_user = 'yulin07'
psql_db = 'yulin07'
psql_password = 'V00786970'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()

if len(sys.argv) < 3:
	print('Usage: %s <course code> <term>'%sys.argv[0], file=sys.stderr)
	sys.exit(0)
	
course_code, term = sys.argv[1:3]

cursor.execute("""select code, student_name, term_code, instructor_name
			from course_offerings
		  where code = %s and term_code = %s;""",(course_code, term))

row = cursor.fetchone()
print_header(row[0],row[1],row[2],row[3])

cursor.execute("""with x as (select id, student_name, code, term_code from students 
				natural join enrollments
					except
	   		     select id, student_name, code, term_code from students
				natural join enrollments natural join grades),
                       y as (select id, student_name, grade, code, term_code from students
				natural join enrollments natural join grades),
                       z as (select id, student_name, null as grade, code, term_code from x),
                       u as (select id, student_name, grade, code, term_code from y
				union
	   		     select id, student_name, null as grade, code, term_code from x)
select id, student_name, grade from u where code = %s and term_code = %s; """, (course_code, term))



students = 0
while True:
	row = cursor.fetchone()
	if row is None:
		break
	print_row(row[0], row[1], row[2])
	students += 1

cursor.execute("""select max_capacity from course_offerings
			where code = %s and term_code = %s;""",(course_code, term))
row = cursor.fetchone()
print_footer(students, row[0])

cursor.close()
conn.close()
