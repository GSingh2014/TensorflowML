import csv
import random

relationships = ['same_route', 'intersect_route', 'independent_route', 'substituted']

with open('C:\\Users\\singhgo\\Documents\\work\\sftp-files\\bus_relation.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    for i in range(1, 20):
        writer.writerow([random.randint(1, 10), random.randint(1, 10), random.choice(relationships)])