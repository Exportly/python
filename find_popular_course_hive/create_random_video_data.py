from random import randint
udata =  open('/Users/nest/Downloads/udemy.tsv', 'w') 
for i in range(1, 100000000):

    userid = randint(1,1000)

    courseid = randint(10,10000)

    pos = randint(0, 150) * 5
    udata.write( "\t".join([str(userid), str(courseid), str(pos)]) + '\n')

udata.close()
