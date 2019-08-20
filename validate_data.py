import csv
import os

def process_file(file_name):
	file = file_name
	out_file = file_name[4:]
	with open(file, 'r') as f:
		with open(out_file, 'w+') as out_file:
			f_reader = csv.reader(f)
			f_writer = csv.writer(out_file)
			# count = 0
			for row in f_reader:
				if len(row) == 11:
					# missing osmething
					# print(len(row))
					f_writer.writerow(row)
			
				else:
					if len(row)<=8:
						# print(len(row))
						# print(row)
						row.insert(7,-2)
					# print(len(row))
					# it's 10 or 9
					# if 8 dont have @, we know they are msg-id and from, add no space
					# print(len(row))
					# print(row)
					if '@' not in row[8]:
						row.insert(7,'')

					if '<' not in row[-1] and len(row)<11:
						row.append('')
					
					# if len(row)<=10:
					# 	row.insert(9,'')
					f_writer.writerow(row)
					# print(row)

os.chdir('data')
for file in os.listdir('.'):
	print(file)
	process_file(file)




