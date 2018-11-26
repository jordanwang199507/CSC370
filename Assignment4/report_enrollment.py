# report_enrollment.py
# CSC 370 - Spring 2018 - Starter code for Assignment 4
# Jordan (Yu-Lin) Wang
# V00786970
# The code below generates a mockup of the output of report_enrollment.py
# as specified in the assignment. You can copy and paste the functions in this
# program into your solution to ensure the correct formatting.
#
# B. Bird - 02/26/2018

import psycopg2, sys

def print_row(term, course_code, course_name, instructor_name, total_enrollment, maximum_capacity):
	print("%6s %10s %-35s %-25s %s/%s"%(str(term), str(course_code), str(course_name), str(instructor_name), str(total_enrollment), str(maximum_capacity)) )

psql_user = 'yulin07'
psql_db = 'yulin07'
psql_password = 'V00786970'
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)
cursor = conn.cursor()

cursor.execute("""with x as (select * from course_offerings),
     		       y as (select code, term_code, count(*) as total_enrollment from enrollments group by code, term_code),
    		       z as (select * from x natural join y),
     	               w as (select term_code, code, student_name, instructor_name, 0 as total_enrollment, max_capacity from course_offerings),
                       u as (select term_code, code from course_offerings 
		                except
	                     select term_code, code from enrollments),
                       t as (select term_code, code, student_name, instructor_name, 0 as total_enrollment, max_capacity from w
		                natural join u)
                  select term_code, code, student_name, instructor_name, total_enrollment, max_capacity from z
	               union
                  select term_code, code, student_name, instructor_name, 0 as total_enrollment, max_capacity from t""")

while True:
 	row = cursor.fetchone()
 	if row is None:
 		break
 	print_row(row[0], row[1], row[2], row[3], row[4], row[5])

cursor.close()
conn.close()
